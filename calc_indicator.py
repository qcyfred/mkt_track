# coding: utf-8
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
from mkt_track_models import (AShareDescription,
                              AIndexEodPrice,
                              AIndexBiasQuantile,
                              AShareBiasQuantile,
                              AIndexBia,
                              AShareBia,
                              AShareEodPrice,
                              ASharePeQuantile,
                              ASharePbQuantile,
                              AShareFinPit,
                              AShareAlphaDd,
                              AShareAlphaDdQuantile)
from mkt_track_utils import trans_number_to_float

# 初始化数据库连接:
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=False)

# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)


def calc_bias(raw_df, win_len=20):
    df = raw_df.copy()
    df['ma_%d' % win_len] = df['close'].rolling(win_len).mean()
    df['value_%d' % win_len] = df['close'] / df['ma_%d' % win_len] - 1

    return df[['value_%d' % win_len]]


# TODO: 装饰器，ensure_list。 是否需要提供复权的选项？
# TODO: 并没有指定起止日期
# TODO: 如果fields是None，就查询全表
def get_equity_market_eod(sec_code, fields=None, is_index=False):
    if isinstance(fields, list):
        pass
    elif isinstance(fields, str):
        fields = [fields]

    if is_index:
        query_fields = [eval('AIndexEodPrice.' + field) for field in fields]
        query_fields.extend([AIndexEodPrice.sec_code, AIndexEodPrice.trade_date])
        sql = select(query_fields).where(AIndexEodPrice.sec_code.__eq__(sec_code)).order_by(AIndexEodPrice.trade_date)
        df = pd.read_sql(sql, engine, index_col='trade_date')
    else:
        query_fields = [eval('AShareEodPrice.' + field) for field in fields]
        query_fields.extend([AShareEodPrice.sec_code, AShareEodPrice.trade_date])
        sql = select(query_fields).where(AShareEodPrice.sec_code.__eq__(sec_code)).order_by(AShareEodPrice.trade_date)
        df = pd.read_sql(sql, engine, index_col='trade_date')

    return df


# 注意，这里只有上证50成分股代码
def get_all_stk_codes():
    sql = select([AShareDescription.sec_code])
    df = pd.read_sql(sql, engine)
    return df['sec_code'].tolist()


def calc_and_save_index_bias(sec_codes):
    for sec_code in sec_codes:
        close_df = get_equity_market_eod(sec_code, fields='close', is_index=True)
        bias_dfs = []
        win_lens = [20, 60, 120]
        for win_len in win_lens:
            bias_df = calc_bias(close_df, win_len)
            bias_dfs.append(bias_df)

        bias_df = pd.concat(bias_dfs, axis=1)
        bias_df['sec_code'] = sec_code
        bias_df.reset_index(inplace=True)

        bias_df.to_sql(AIndexBia.__tablename__, engine, index=False, if_exists='append')


# 个股bias
# TODO: Bug: 股票的bias，要用复权的收盘价来计算
def calc_and_save_stock_bias(sec_codes):
    for sec_code in sec_codes:
        df = get_equity_market_eod(sec_code, fields=['close', 'adjfactor'], is_index=False)
        df['close'] = df['close'] * df['adjfactor']  # 后复权
        bias_dfs = []
        win_lens = [20, 60, 120]
        for win_len in win_lens:
            bias_df = calc_bias(df[['close']], win_len)  # 这里是后复权价
            bias_dfs.append(bias_df)

        bias_df = pd.concat(bias_dfs, axis=1)
        bias_df['sec_code'] = sec_code
        bias_df.reset_index(inplace=True)

        bias_df.to_sql(AShareBia.__tablename__, engine, index=False, if_exists='append')


def get_bias_eod(sec_code, is_index=False):
    if is_index:
        sql = select([AIndexBia]).where(AIndexBia.sec_code.__eq__(sec_code))
        bias_df = pd.read_sql(sql, engine, index_col='trade_date')
    else:
        sql = select([AShareBia]).where(AShareBia.sec_code.__eq__(sec_code))
        bias_df = pd.read_sql(sql, engine, index_col='trade_date')

    return bias_df


def calc_and_save_index_bias_quantile():
    observation_period = 250
    win_lens = [20, 60, 120]

    session = DBSession()

    sec_codes = ['000016.SH', '000300.SH', '000905.SH']
    for sec_code in sec_codes:
        bias_df = get_bias_eod(sec_code, is_index=True)
        bias_df = bias_df.iloc[-observation_period:].copy()  # 观察期内所有bias。 注意，sort会改变原来的值
        last_bias_quantile_dict = dict()
        last_trade_date = bias_df.index[-1]
        for win_len in win_lens:
            bias_df['value_%d' % win_len] = bias_df['value_%d' % win_len].abs()  # bias全部转成 非负数
            last_bias = bias_df['value_%d' % win_len][-1]
            bias_arr = bias_df['value_%d' % win_len].values
            bias_arr.sort()  # 注意，会改变bias_arr本来的内容
            if np.isnan(last_bias):
                raise ValueError('最后一天last_bias是nan，检查数据')
            last_bias_idx = np.argwhere(bias_arr == last_bias)[0][0]
            last_bias_quantile = last_bias_idx / observation_period
            last_bias_quantile_dict[win_len] = last_bias_quantile  # 分位数

        new_obj = AIndexBiasQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.value_20 = trans_number_to_float(last_bias_quantile_dict[20])
        new_obj.value_60 = trans_number_to_float(last_bias_quantile_dict[60])
        new_obj.value_120 = trans_number_to_float(last_bias_quantile_dict[120])
        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


def calc_and_save_stock_bias_quantile():
    observation_period = 250
    win_lens = [20, 60, 120]

    session = DBSession()

    sql = select([AShareDescription.sec_code])
    df = pd.read_sql(sql, engine)
    sec_codes = df['sec_code'].tolist()

    for sec_code in sec_codes:
        bias_df = get_bias_eod(sec_code, is_index=False)
        bias_df = bias_df.iloc[-observation_period:].copy()  # 观察期内所有bias。 注意，sort会改变原来的值
        last_bias_quantile_dict = dict()
        last_trade_date = bias_df.index[-1]
        for win_len in win_lens:
            bias_df['value_%d' % win_len] = bias_df['value_%d' % win_len].abs()  # bias全部转成 非负数
            last_bias = bias_df['value_%d' % win_len][-1]
            bias_arr = bias_df['value_%d' % win_len].values
            bias_arr.sort()  # 注意，会改变bias_arr本来的内容
            if np.isnan(last_bias):
                raise ValueError('最新数据是nan，检查数据')
            last_bias_idx = np.argwhere(bias_arr == last_bias)[0][0]
            last_bias_quantile = last_bias_idx / observation_period
            last_bias_quantile_dict[win_len] = last_bias_quantile  # 分位数

        new_obj = AShareBiasQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.value_20 = trans_number_to_float(last_bias_quantile_dict[20])
        new_obj.value_60 = trans_number_to_float(last_bias_quantile_dict[60])
        new_obj.value_120 = trans_number_to_float(last_bias_quantile_dict[120])
        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


def get_a_share_fin_pit(sec_code):
    sql = select([AShareFinPit]).where(
        AShareFinPit.sec_code.__eq__(sec_code))
    df = pd.read_sql(sql, engine, index_col='trade_date')
    return df


def calc_and_save_stock_pe_quantile():
    observation_period = 750

    session = DBSession()

    sql = select([AShareDescription.sec_code])
    df = pd.read_sql(sql, engine)
    sec_codes = df['sec_code'].tolist()

    for sec_code in sec_codes:
        value_df = get_a_share_fin_pit(sec_code)
        value_df = value_df.iloc[-observation_period:].copy()  # 注意，sort会改变原来的值
        last_trade_date = value_df.index[-1]
        last_value = value_df['pe_ttm'][-1]
        value_arr = value_df['pe_ttm'].values
        value_arr.sort()  # 注意，会改变bias_arr本来的内容
        if np.isnan(last_value):
            raise ValueError('最新数据是nan，检查数据')
        last_value_idx = np.argwhere(value_arr == last_value)[0][0]
        last_value_quantile = last_value_idx / observation_period

        new_obj = ASharePeQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.val = trans_number_to_float(last_value_quantile)
        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


def calc_and_save_stock_pb_quantile():
    observation_period = 750

    session = DBSession()

    sql = select([AShareDescription.sec_code])
    df = pd.read_sql(sql, engine)
    sec_codes = df['sec_code'].tolist()

    for sec_code in sec_codes:
        value_df = get_a_share_fin_pit(sec_code)
        value_df = value_df.iloc[-observation_period:].copy()  # sort会改变原来的值
        last_trade_date = value_df.index[-1]
        last_value = value_df['pb'][-1]
        value_arr = value_df['pb'].values
        value_arr.sort()  # 注意，会改变bias_arr本来的内容
        if np.isnan(last_value):
            raise ValueError('最新数据是nan，检查数据')
        last_value_idx = np.argwhere(value_arr == last_value)[0][0]
        last_value_quantile = last_value_idx / observation_period

        new_obj = ASharePbQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.val = trans_number_to_float(last_value_quantile)
        new_obj.observation_period = observation_period
        session.add(new_obj)
    session.commit()
    session.close()


# # 指数bias
sec_codes = ['000016.SH', '000300.SH', '000905.SH']
calc_and_save_index_bias(sec_codes)  # 指数乖离率

stk_codes = get_all_stk_codes()
calc_and_save_stock_bias(stk_codes)  # 股票乖离率

calc_and_save_index_bias_quantile()  # 指数乖离率分位数
calc_and_save_stock_bias_quantile()  # 股票乖离率分位数

calc_and_save_stock_pe_quantile()  # 股票pe分位数
calc_and_save_stock_pb_quantile()  # 股票pb分位数


def get_a_share_eod():
    fields = ['pct_chg']
    query_fields = [eval('AShareEodPrice.' + field) for field in fields]
    query_fields.extend([AShareEodPrice.sec_code, AShareEodPrice.trade_date])
    sql = select(query_fields)
    df = pd.read_sql(sql, engine, index_col='trade_date')
    df = df.pivot(columns='sec_code', values='pct_chg')

    return df


# 超额收益最近20日的dd
def calc_dd(s):
    max_value = s.max()
    dd = s[-1] / max_value - 1
    return dd


# 个股超额收益的近期回撤
def calc_and_save_dd():
    win_len = 20
    df = get_a_share_eod()
    index_df = get_equity_market_eod('000016.SH', 'pct_chg', is_index=True)
    alpha_pct_df = pd.DataFrame(df.values - index_df[['pct_chg']].values, columns=df.columns, index=df.index)
    alpha_df = alpha_pct_df.divide(100)
    alpha_unit_value_df = (1 + alpha_df).cumprod()

    dd_df = alpha_unit_value_df.rolling(win_len).apply(calc_dd)
    # print(dd_df)

    dd_mat = dd_df.values
    trade_dates = dd_df.index
    sec_codes = dd_df.columns
    session = DBSession()

    for i in range(len(trade_dates)):
        for j in range(len(sec_codes)):
            trade_date = trade_dates[i]
            sec_code = sec_codes[j]

            new_obj = AShareAlphaDd()
            new_obj.sec_code = sec_code
            new_obj.trade_date = trade_date
            new_obj.value_20 = trans_number_to_float(dd_mat[i, j])

            session.add(new_obj)

    session.commit()
    session.close()


def get_a_share_alpha_dd(sec_code):
    sql = select([AShareAlphaDd]).where(
        AShareAlphaDd.sec_code.__eq__(sec_code))
    df = pd.read_sql(sql, engine, index_col='trade_date')
    return df


# 个股超额收益的近期回撤的超额收益
def calc_and_save_alpha_dd_quantile():
    observation_period = 250
    win_len = 20

    session = DBSession()

    sec_codes = get_all_stk_codes()

    for sec_code in sec_codes:
        value_df = get_a_share_alpha_dd(sec_code)
        value_df = value_df.iloc[-observation_period:].copy()  # 注意，sort会改变原来的值

        value_df['value_%d' % win_len] = value_df['value_%d' % win_len].abs()
        last_trade_date = value_df.index[-1]
        last_value = value_df['value_%d' % win_len][-1]
        # 回撤为0的数据，分位数为0
        if last_value < 1e-6:
            last_value_quantile = 0
        else:
            value_arr = value_df['value_%d' % win_len].values
            value_arr = np.array([x for x in value_arr if x > 1e-6])
            value_arr.sort()  # 注意，会改变bias_arr本来的内容
            if np.isnan(last_value):
                raise ValueError('最新数据是nan，检查数据')
            last_value_idx = np.argwhere(value_arr == last_value)[0][0]
            last_value_quantile = last_value_idx / observation_period

        new_obj = AShareAlphaDdQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.value_20 = trans_number_to_float(last_value_quantile)
        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


calc_and_save_dd()  # 个股超额收益的近期回撤
calc_and_save_alpha_dd_quantile()  # 个股超额收益的近期回撤区间内分位数
