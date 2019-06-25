# coding: utf-8
import pandas as pd
import numpy as np
import ffn
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy import (select,
                        and_)
from mkt_track_models import (ChinaEtfPchRedmList,
                              ChinaEtfPrevWeight,
                              AShareAlphaQuantile,
                              AIndexEodPrice,
                              AShareEodPrice)
import datetime

# 分位数

# 初始化数据库连接
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track_bt?charset=utf8', echo=False)

# TODO：修改回测起始日期
begin_yyyymmdd = '20170703'
end_yyyymmdd = '20190620'

t1 = datetime.datetime.now()

# 所有交易日
sql = select([ChinaEtfPchRedmList.trade_date]).where(ChinaEtfPchRedmList.etf_sec_code == '510050.SH').distinct(
    ChinaEtfPchRedmList.trade_date)
df = pd.read_sql(sql, engine)
trade_dates = df['trade_date'].tolist()

# 每天各股票的alpha分位数
sql = select([AShareAlphaQuantile]).order_by(AShareAlphaQuantile.sec_code)
df = pd.read_sql(sql, engine)
df = df.pivot(columns='sec_code', index='trade_date', values='value_20')

pos_multiplier_df = pd.DataFrame(np.ones(df.shape),
                                 columns=df.columns,
                                 index=df.index)
# 抄底摸顶
pos_multiplier_df[df > 0.9] = 0
pos_multiplier_df[df < 0.1] = 2

# 追涨杀跌
# pos_multiplier_df[df > 0.8] = 2
# pos_multiplier_df[df < 0.1] = 0

all_sec_codes = df.columns

pos_multiplier_df = pos_multiplier_df - 1

# 上证50成分股每日清单（股数）
sql = select([ChinaEtfPchRedmList.sec_code,
              ChinaEtfPchRedmList.trade_date,
              ChinaEtfPchRedmList.volume]).where(
    and_(ChinaEtfPchRedmList.trade_date >= begin_yyyymmdd,
         ChinaEtfPchRedmList.trade_date <= end_yyyymmdd)).order_by(ChinaEtfPchRedmList.sec_code)
origin_pos_df = pd.read_sql(sql, engine)
origin_pos_df = origin_pos_df.pivot(index='trade_date', columns='sec_code', values='volume')

# 上证50成分股的每日涨跌幅
sql = select((AShareEodPrice.sec_code, AShareEodPrice.trade_date, AShareEodPrice.pct_chg)).where(
    and_(AShareEodPrice.sec_code.in_(all_sec_codes),
         AShareEodPrice.trade_date >= begin_yyyymmdd,
         AShareEodPrice.trade_date <= end_yyyymmdd)).order_by(
    AShareEodPrice.sec_code)
ret_df = pd.read_sql(sql, engine)
ret_df = ret_df.pivot(columns='sec_code', index='trade_date', values='pct_chg')
ret_df = ret_df / 100

# 指数收益率
sql = select([AIndexEodPrice.trade_date, AIndexEodPrice.pct_chg]).where(
    and_(AIndexEodPrice.sec_code == '000016.SH',
         AIndexEodPrice.trade_date >= begin_yyyymmdd,
         AIndexEodPrice.trade_date <= end_yyyymmdd))
idx_ret_df = pd.read_sql(sql, engine, index_col='trade_date')
idx_ret_df = idx_ret_df.divide(100)
idx_ret = idx_ret_df['pct_chg']

# 策略的持仓
pos_df = origin_pos_df.copy() * pos_multiplier_df
pos_df.fillna(0, inplace=True)

# 个股价格，用金额计算
sql = select((AShareEodPrice.sec_code, AShareEodPrice.trade_date, AShareEodPrice.close)).where(
    and_(AShareEodPrice.sec_code.in_(all_sec_codes),
         AShareEodPrice.trade_date >= begin_yyyymmdd,
         AShareEodPrice.trade_date <= end_yyyymmdd)).order_by(
    AShareEodPrice.sec_code)
px_df = pd.read_sql(sql, engine)
px_df = px_df.pivot(columns='sec_code', index='trade_date', values='close')

amt_df = pos_df * px_df

# 策略净值、收益
daily_pnl = (amt_df.shift(1) * ret_df).sum(axis=1)
pnl_df = daily_pnl.cumsum()
pnl_df.index = pd.to_datetime(pnl_df.index)
fig2 = plt.figure(figsize=(12, 8))
plt.plot(pnl_df)
plt.savefig('pnl_df.png')

hedge_daily_pnl = daily_pnl - amt_df.sum(axis=1).shift(1) * idx_ret
hedge_pnl = hedge_daily_pnl.cumsum()
fig3 = plt.figure(figsize=(12, 8))
plt.plot(hedge_pnl)
plt.savefig('hedge_pnl.png')
