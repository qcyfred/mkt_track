# coding: utf-8
# TODO: 注意起止日期
from sqlalchemy import create_engine
from sqlalchemy import (select,
                        and_)
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
from functools import reduce
from mkt_track_models import (AShareAlpha,
                              AShareAlphaQuantile,
                              ASse50Description,
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
                              AShareAlphaDdQuantile,
                              ChinaEtfPchRedmList)
from mkt_track_utils import trans_number_to_float

# 初始化数据库连接:
# engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=True)
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track_bt?charset=utf8', echo=True)

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


def get_sse50_stk_codes(trade_date):
    sql = select([ChinaEtfPchRedmList.sec_code]).where(and_(ChinaEtfPchRedmList.trade_date == trade_date))
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


def get_bias_eod(sec_code, end_yyyymmdd, is_index=False):
    if is_index:
        sql = select([AIndexBia]).where(and_(AIndexBia.sec_code.__eq__(sec_code), AIndexBia.trade_date <= end_yyyymmdd))
        bias_df = pd.read_sql(sql, engine, index_col='trade_date')
    else:
        sql = select([AShareBia]).where(and_(AShareBia.sec_code.__eq__(sec_code), AShareBia.trade_date <= end_yyyymmdd))
        bias_df = pd.read_sql(sql, engine, index_col='trade_date')

    return bias_df


# bias加了abs
def calc_and_save_index_bias_quantile(end_yyymmdd):
    observation_period = 250
    win_lens = [20, 60, 120]

    session = DBSession()

    sec_codes = ['000016.SH', '000300.SH', '000905.SH']
    for sec_code in sec_codes:
        bias_df = get_bias_eod(sec_code, end_yyymmdd, is_index=True)
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


# bias加了abs
def calc_and_save_stock_bias_quantile(end_yyymmdd):
    observation_period = 250
    win_lens = [20, 60, 120]

    session = DBSession()

    # TODO: ，每天50的成分股代码还不一样！
    sec_codes = get_sse50_stk_codes()

    for sec_code in sec_codes:
        bias_df = get_bias_eod(sec_code, end_yyymmdd, is_index=False)
        bias_df = bias_df.iloc[-observation_period:].copy()  # 观察期内所有bias。 注意，sort会改变原来的值
        last_bias_quantile_dict = dict()
        last_trade_date = bias_df.index[-1]
        for win_len in win_lens:
            print(sec_code, win_len)
            if bias_df['value_%d' % win_len].all(None):  # 如果全是None（如刚刚上市，或连续停牌）
                bias_df['value_%d' % win_len] = None
                last_bias_quantile_dict[win_len] = None
            else:
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

    sec_codes = get_sse50_stk_codes()

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

    sec_codes = get_sse50_stk_codes()

    for sec_code in sec_codes:
        value_df = get_a_share_fin_pit(sec_code)
        value_df = value_df.iloc[-observation_period:].copy()  # sort会改变原来的值
        last_trade_date = value_df.index[-1]
        last_value = value_df['pb'][-1]
        value_arr = value_df['pb'].values
        value_arr.sort()  # 注意，sort会改变bias_arr本来的内容
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


# 注意起止日期
# TODO：每天成分股不一样！
def get_a_sse50_pct_chg(end_yyyymmdd):
    # 当天成分股
    sec_codes = get_sse50_stk_codes(end_yyyymmdd)

    fields = ['pct_chg']
    query_fields = [eval('AShareEodPrice.' + field) for field in fields]
    query_fields.extend([AShareEodPrice.sec_code, AShareEodPrice.trade_date])
    sql = select(query_fields).where(and_(AShareEodPrice.trade_date >= '20160601',
                                          AShareEodPrice.sec_code.in_(sec_codes)))
    df = pd.read_sql(sql, engine, index_col='trade_date')
    df = df.pivot(columns='sec_code', values='pct_chg')

    return df


def save_df_into_db(df, clazz):
    mat = df.values
    trade_dates = df.index
    sec_codes = df.columns
    session = DBSession()

    for i in range(len(trade_dates)):
        for j in range(len(sec_codes)):
            trade_date = trade_dates[i]
            sec_code = sec_codes[j]

            new_obj = clazz()
            new_obj.sec_code = sec_code
            new_obj.trade_date = trade_date
            new_obj.value_20 = trans_number_to_float(mat[i, j])

            session.add(new_obj)

    session.commit()
    session.close()


# 所有股票的收益率
def get_all_stk_pct_chg():
    fields = ['pct_chg']
    query_fields = [eval('AShareEodPrice.' + field) for field in fields]
    query_fields.extend([AShareEodPrice.sec_code, AShareEodPrice.trade_date])
    sql = select(query_fields).where(and_(AShareEodPrice.trade_date >= '20160601'))
    df = pd.read_sql(sql, engine, index_col='trade_date')
    df = df.pivot(columns='sec_code', values='pct_chg')

    return df


# 计算个股每日的超额收益，个股涨跌幅与指数相减
# 注意：这里是否需要考虑，看交易信号，用20日涨跌幅相除。算收益，才用对冲的算法。
def calc_daily_alpha():
    # df = get_a_sse50_pct_chg(end_yyyymmdd)
    df = get_all_stk_pct_chg()
    index_df = get_equity_market_eod('000016.SH', 'pct_chg', is_index=True)
    alpha_pct_df = pd.DataFrame(df.values - index_df[['pct_chg']].values, columns=df.columns, index=df.index)
    alpha_df = alpha_pct_df.divide(100)
    return alpha_df


def get_a_share_alpha(sec_code, end_yyyymmdd):
    sql = select([AShareAlpha]).where(
        and_(AShareAlpha.sec_code.__eq__(sec_code),
             AShareAlpha.trade_date <= end_yyyymmdd))
    df = pd.read_sql(sql, engine, index_col='trade_date')
    return df


# 个股超额收益的分位数（不加abs）
def calc_and_save_alpha_quantile(end_yyyymmdd):
    observation_period = 250

    session = DBSession()
    sec_codes = get_sse50_stk_codes(end_yyyymmdd)

    for sec_code in sec_codes:
        print(sec_code, end_yyyymmdd)
        value_df = get_a_share_alpha(sec_code, end_yyyymmdd)
        value_df = value_df.iloc[-observation_period:].copy()  # 注意，sort会改变原来的值

        win_len = 20  # 20日

        last_trade_date = value_df.index[-1]
        last_value = value_df['value_%d' % win_len][-1]
        value_arr = value_df['value_%d' % win_len].values
        value_arr = np.array([x for x in value_arr if x > 1e-6])
        value_arr.sort()  # 注意，会改变bias_arr本来的内容
        if np.isnan(last_value):
            raise ValueError('最新数据是nan，检查数据')
        last_value_idx = np.argwhere(value_arr == last_value)[0][0]
        last_value_quantile = last_value_idx / observation_period

        new_obj = AShareAlphaQuantile()
        new_obj.sec_code = sec_code
        new_obj.trade_date = last_trade_date
        new_obj.value_20 = trans_number_to_float(last_value_quantile)  # 修改窗口长度

        #############################
        win_len = 60  # 60日
        last_value = value_df['value_%d' % win_len][-1]
        value_arr = value_df['value_%d' % win_len].values
        value_arr = np.array([x for x in value_arr if x > 1e-6])
        value_arr.sort()  # 注意，会改变bias_arr本来的内容
        if np.isnan(last_value):
            raise ValueError('最新数据是nan，检查数据')
        last_value_idx = np.argwhere(value_arr == last_value)[0][0]
        last_value_quantile = last_value_idx / observation_period

        new_obj.value_60 = trans_number_to_float(last_value_quantile)  # 修改窗口长度

        new_obj.observation_period = observation_period
        session.add(new_obj)

    session.commit()
    session.close()


# 超额收益最近20日的dd
def calc_dd(s):
    max_value = s.max()
    dd = s[-1] / max_value - 1
    return dd


# 计算数组里所有元素的乘积
def calc_prod(s):
    prod = reduce(lambda x, y: x * y, s)
    return prod


# 个股T个交易日，滚动的超额收益
def calc_and_save_rolling_alpha(daily_alpha_df):
    # 注意：这里修改窗口长度
    alpha_rolling_unit_value_df_20 = (1 + daily_alpha_df).rolling(20).apply(calc_prod, raw=True)
    alpha_rolling_unit_value_df_60 = (1 + daily_alpha_df).rolling(60).apply(calc_prod, raw=True)
    # save_df_into_db(alpha_rolling_unit_value_df, AShareAlpha)

    df_20 = alpha_rolling_unit_value_df_20
    mat_20 = df_20.values
    mat_60 = alpha_rolling_unit_value_df_60.values
    trade_dates = df_20.index
    sec_codes = df_20.columns
    session = DBSession()

    for i in range(len(trade_dates)):
        for j in range(len(sec_codes)):
            trade_date = trade_dates[i]
            sec_code = sec_codes[j]

            new_obj = AShareAlpha()
            new_obj.sec_code = sec_code
            new_obj.trade_date = trade_date
            new_obj.value_20 = trans_number_to_float(mat_20[i, j])
            new_obj.value_60 = trans_number_to_float(mat_60[i, j])

            session.add(new_obj)

    session.commit()
    session.close()


# 个股超额收益的近期回撤
def calc_and_save_alpha_dd(daily_alpha_df):
    win_len = 20
    alpha_unit_value_df = (1 + daily_alpha_df).cumprod()
    dd_df = alpha_unit_value_df.rolling(win_len).apply(calc_dd, raw=True)
    save_df_into_db(dd_df, AShareAlphaDd)


def get_a_share_alpha_dd(sec_code):
    sql = select([AShareAlphaDd]).where(
        AShareAlphaDd.sec_code.__eq__(sec_code))
    df = pd.read_sql(sql, engine, index_col='trade_date')
    return df


# 个股超额收益的近期回撤的分位数（加了abs）
def calc_and_save_alpha_dd_quantile():
    observation_period = 250
    win_len = 20

    session = DBSession()

    sec_codes = get_sse50_stk_codes()

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


# 清除数据库的计算结果
def truncate_db():
    sql_str = """
    # TRUNCATE a_index_bias;
    # TRUNCATE a_index_bias_quantile;
    # TRUNCATE a_share_bias;
    # TRUNCATE a_share_bias_quantile;
    # TRUNCATE a_share_pb_quantile;
    # TRUNCATE a_share_pe_quantile;
    TRUNCATE a_share_alpha;
    TRUNCATE a_share_alpha_quantile;
    # TRUNCATE a_share_alpha_dd;
    # TRUNCATE a_share_alpha_dd_quantile;
    """
    sqls = sql_str.split('\n')
    for sql in sqls:
        print(sql)
        engine.execute(sql)


# truncate_db()

# 指数bias
# index_codes = ['000016.SH', '000300.SH', '000905.SH']
# calc_and_save_index_bias(index_codes)  # 指数乖离率
#
# stk_codes = get_sse50_stk_codes()
# calc_and_save_stock_bias(stk_codes)  # 股票乖离率

# calc_and_save_index_bias_quantile(end_yyymmdd='20190506')  # 指数乖离率分位数
# calc_and_save_stock_bias_quantile(end_yyymmdd='20190506')  # 股票乖离率分位数

# calc_and_save_stock_pe_quantile()  # 股票pe分位数
# calc_and_save_stock_pb_quantile()  # 股票pb分位数


#######################
daily_alpha_df = calc_daily_alpha()
calc_and_save_rolling_alpha(daily_alpha_df)  # 个股超额收益曲线（滚动T日）

############# 每天成分股的分位数
sql = select([AIndexEodPrice.trade_date]).where(and_(AIndexEodPrice.sec_code == '000016.SH',
                                                     AIndexEodPrice.trade_date >= '20170101')).distinct(
    AIndexEodPrice.trade_date)
df = pd.read_sql(sql, engine)
trade_dates = df['trade_date'].tolist()
for trade_date in trade_dates:
    calc_and_save_alpha_quantile(trade_date)  # 个股超额收益分位数（滚动T日）

# calc_and_save_alpha_dd(daily_alpha_df)  # 个股超额收益的近期回撤
# calc_and_save_alpha_dd_quantile()  # 个股超额收益的近期回撤区间内分位数
