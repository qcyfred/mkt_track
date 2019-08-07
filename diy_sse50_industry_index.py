# coding: utf-8
# 上证50成分股行业指数
import pandas as pd
import numpy as np
# import ffn
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy import (select,
                        and_)
from mkt_track_bt_models import (ChinaEtfPchRedmList,
                                 AShareIndustry,
                                 AShareEodPrice)

# 初始化数据库连接
engine = create_engine('mysql+pymysql://ljm:ljm@192.168.119.107:3306/db_mkt_track_bt?charset=utf8', echo=False)

# TODO：修改起始日期
begin_yyyymmdd = '20170703'
end_yyyymmdd = '20190620'

# 所有交易日
sql = select([ChinaEtfPchRedmList.trade_date]).where(ChinaEtfPchRedmList.etf_sec_code == '510050.SH').distinct(
    ChinaEtfPchRedmList.trade_date)
df = pd.read_sql(sql, engine)
trade_dates = df['trade_date'].tolist()

# 银行业的股票
sql = select([AShareIndustry.sec_code,
              AShareIndustry.ind_name]).where(
    AShareIndustry.ind_name == '银行').order_by(AShareIndustry.sec_code)
ind_cpnt_stk_df = pd.read_sql(sql, engine)
print(ind_cpnt_stk_df)

ind_cpnt_sec_codes = ind_cpnt_stk_df['sec_code'].tolist()

# 每日申赎清单
sql = select([ChinaEtfPchRedmList.sec_code,
              ChinaEtfPchRedmList.trade_date,
              ChinaEtfPchRedmList.volume]).where(
    and_(ChinaEtfPchRedmList.trade_date >= begin_yyyymmdd,
         ChinaEtfPchRedmList.trade_date <= end_yyyymmdd,
         ChinaEtfPchRedmList.sec_code.in_(ind_cpnt_sec_codes))).order_by(ChinaEtfPchRedmList.sec_code)
origin_pos_df = pd.read_sql(sql, engine)
origin_pos_df = origin_pos_df.pivot(index='trade_date', columns='sec_code', values='volume')
origin_pos_df.fillna(0, inplace=True)

print(origin_pos_df)

all_sec_codes = list(origin_pos_df.columns)

# 股票每日价格（不复权）和涨跌幅
sql = select([AShareEodPrice.sec_code,
              AShareEodPrice.trade_date,
              AShareEodPrice.close,
              AShareEodPrice.pct_chg]).where(
    and_(AShareEodPrice.trade_date >= begin_yyyymmdd,
         AShareEodPrice.trade_date <= end_yyyymmdd,
         AShareEodPrice.sec_code.in_(all_sec_codes))).order_by(AShareEodPrice.sec_code)
temp_df = pd.read_sql(sql, engine)
temp_df.fillna(0, inplace=True)
px_df = temp_df.copy()
del px_df['pct_chg']
px_df = px_df.pivot(index='trade_date', columns='sec_code', values='close')

ret_df = temp_df.copy()
del ret_df['close']
ret_df = ret_df.pivot(index='trade_date', columns='sec_code', values='pct_chg')
ret_df = ret_df / 100  # 百分比换成小数

# 计算每日权重（T日开盘权重）
amt_df = origin_pos_df.multiply(px_df)
weight_pos_df = amt_df.div(amt_df.sum(axis=1), axis=0)
print(weight_pos_df)

# 行业全收益指数
daily_ret = weight_pos_df.multiply(ret_df).sum(axis=1)
industry_index = (1 + daily_ret).cumprod()
print(industry_index)

# plot
fig = plt.figure(1, figsize=(8, 6))
plt.plot(industry_index)
plt.show()
