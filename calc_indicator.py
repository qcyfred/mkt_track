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
                              AShareFinPit)
from mkt_track_utils import trans_number_type

# 初始化数据库连接:
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=False)

# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)


def calc_bias(raw_df, win_len=20):
    df = raw_df.copy()
    df['ma_%d' % win_len] = df['close'].rolling(win_len).mean()
    df['value_%d' % win_len] = df['close'] / df['ma_%d' % win_len] - 1

    return df[['value_%d' % win_len]]


def get_equity_market_eod(sec_code, is_index=False):
    if is_index:
        sql = select([AIndexEodPrice.trade_date, AIndexEodPrice.close]).where(AIndexEodPrice.sec_code.__eq__(sec_code))
        df = pd.read_sql(sql, engine, index_col='trade_date')
    else:
        sql = select([AShareEodPrice.trade_date, AShareEodPrice.close]).where(AShareEodPrice.sec_code.__eq__(sec_code))
        df = pd.read_sql(sql, engine, index_col='trade_date')

    return df


def calc_and_save_index_bias(sec_codes):
    for sec_code in sec_codes:
        close_df = get_equity_market_eod(sec_code, is_index=True)
        bias_dfs = []
        win_lens = [20, 60, 120]
        for win_len in win_lens:
            bias_df = calc_bias(close_df, win_len)
            bias_dfs.append(bias_df)

        bias_df = pd.concat(bias_dfs, axis=1)
        bias_df['sec_code'] = sec_code
        bias_df.reset_index(inplace=True)

        bias_df.to_sql(AIndexBia.__tablename__, engine, index=False, if_exists='append')


# # 指数bias
sec_codes = ['000016.SH', '000300.SH', '000905.SH']
calc_and_save_index_bias(sec_codes)  # 指数乖离率


# 个股bias
def calc_and_save_stock_bias(sec_codes):
    for sec_code in sec_codes:
        close_df = get_equity_market_eod(sec_code, is_index=False)
        bias_dfs = []
        win_lens = [20, 60, 120]
        for win_len in win_lens:
            bias_df = calc_bias(close_df, win_len)
            bias_dfs.append(bias_df)

        bias_df = pd.concat(bias_dfs, axis=1)
        bias_df['sec_code'] = sec_code
        bias_df.reset_index(inplace=True)

        bias_df.to_sql(AShareBia.__tablename__, engine, index=False, if_exists='append')


sql = select([AShareDescription.sec_code])
df = pd.read_sql(sql, engine)
stk_codes = df['sec_code'].tolist()
calc_and_save_stock_bias(stk_codes)  # 股票乖离率


def get_bias_eod(sec_code, is_index=False):
    if is_index:
        sql = select([AIndexBia]).where(AIndexBia.sec_code.__eq__(sec_code))
        bias_df = pd.read_sql(sql, engine, index_col='trade_date')
    else:
        sql = select([AShareBia]).where(AShareBia.sec_code.__eq__(sec_code))
        bias_df = pd.read_sql(sql, engine, index_col='trade_date')

    return bias_df


def calc_and_save_index_bias_quantile():
    observation_period = 120
    win_lens = [20, 60, 120]

    session = DBSession()

    sec_codes = ['000016.SH', '000300.SH', '000905.SH']
    for sec_code in sec_codes:
        bias_df = get_bias_eod(sec_code, is_index=True)
        bias_df = bias_df.iloc[-observation_period:].copy()  # 观察期内所有bias。 注意，sort会改变原来的值
        last_bias_quantile_dict = dict()
        last_trade_date = bias_df.index[-1]
        for win_len in win_lens:
            last_bias = bias_df['value_%d' % win_len][-1]
            bias_arr = bias_df['value_%d' % win_len].values
            bias_arr.sort()  # 注意，会改变bias_arr本来的内容
            last_bias_idx = np.argwhere(bias_arr == last_bias)[0][0]
            last_bias_quantile = last_bias_idx / observation_period
            last_bias_quantile_dict[win_len] = last_bias_quantile  # 分位数

        new_obj = AIndexBiasQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.value_20 = trans_number_type(last_bias_quantile_dict[20])
        new_obj.value_60 = trans_number_type(last_bias_quantile_dict[60])
        new_obj.value_120 = trans_number_type(last_bias_quantile_dict[120])
        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


def calc_and_save_stock_bias_quantile():
    observation_period = 120
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
            last_bias = bias_df['value_%d' % win_len][-1]
            bias_arr = bias_df['value_%d' % win_len].values
            bias_arr.sort()  # 注意，会改变bias_arr本来的内容
            last_bias_idx = np.argwhere(bias_arr == last_bias)[0][0]
            last_bias_quantile = last_bias_idx / observation_period
            last_bias_quantile_dict[win_len] = last_bias_quantile  # 分位数

        new_obj = AShareBiasQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.value_20 = trans_number_type(last_bias_quantile_dict[20])
        new_obj.value_60 = trans_number_type(last_bias_quantile_dict[60])
        new_obj.value_120 = trans_number_type(last_bias_quantile_dict[120])
        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


calc_and_save_index_bias_quantile()  # 指数乖离率分位数
calc_and_save_stock_bias_quantile()  # 股票乖离率分位数


def get_share_fin_pit(sec_code):
    sql = select([AShareFinPit]).where(
        AShareFinPit.sec_code.__eq__(sec_code))
    df = pd.read_sql(sql, engine, index_col='trade_date')
    return df


def calc_and_save_stock_pe_quantile():
    observation_period = 120
    win_lens = [20, 60, 120]

    session = DBSession()

    sql = select([AShareDescription.sec_code])
    df = pd.read_sql(sql, engine)
    sec_codes = df['sec_code'].tolist()

    for sec_code in sec_codes:
        value_df = get_share_fin_pit(sec_code)
        value_df = value_df.iloc[-observation_period:].copy()  # 观察期内所有bias。 注意，sort会改变原来的值
        last_value_quantile_dict = dict()
        last_trade_date = value_df.index[-1]
        for win_len in win_lens:
            last_value = value_df['pe_ttm'][-1]
            value_arr = value_df['pe_ttm'].values
            value_arr.sort()  # 注意，会改变bias_arr本来的内容
            last_value_idx = np.argwhere(value_arr == last_value)[0][0]
            last_value_quantile = last_value_idx / observation_period
            last_value_quantile_dict[win_len] = last_value_quantile  # 分位数

        new_obj = ASharePeQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.value_20 = trans_number_type(last_value_quantile_dict[20])
        new_obj.value_60 = trans_number_type(last_value_quantile_dict[60])
        new_obj.value_120 = trans_number_type(last_value_quantile_dict[120])
        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


def calc_and_save_stock_pb_quantile():
    observation_period = 120
    win_lens = [20, 60, 120]

    session = DBSession()

    sql = select([AShareDescription.sec_code])
    df = pd.read_sql(sql, engine)
    sec_codes = df['sec_code'].tolist()

    for sec_code in sec_codes:
        value_df = get_share_fin_pit(sec_code)
        value_df = value_df.iloc[-observation_period:].copy()  # 观察期内所有bias。 注意，sort会改变原来的值
        last_value_quantile_dict = dict()
        last_trade_date = value_df.index[-1]
        for win_len in win_lens:
            last_value = value_df['pb_ttm'][-1]
            value_arr = value_df['pb_ttm'].values
            value_arr.sort()  # 注意，会改变bias_arr本来的内容
            last_value_idx = np.argwhere(value_arr == last_value)[0][0]
            last_value_quantile = last_value_idx / observation_period
            last_value_quantile_dict[win_len] = last_value_quantile  # 分位数

        new_obj = ASharePbQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.value_20 = trans_number_type(last_value_quantile_dict[20])
        new_obj.value_60 = trans_number_type(last_value_quantile_dict[60])
        new_obj.value_120 = trans_number_type(last_value_quantile_dict[120])
        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


calc_and_save_stock_pe_quantile()  # 股票pe分位数
calc_and_save_stock_pb_quantile()  # 股票pb分位数
