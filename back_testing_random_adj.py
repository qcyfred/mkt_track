# coding: utf-8
# TODO: 随机超配或低配，回测
import random
from sqlalchemy import create_engine
from sqlalchemy import (select,
                        and_)
import pandas as pd
from mkt_track_models import (ChinaEtfPchRedmList,
                              ChinaEtfPrevWeight,
                              )
from md_stock_models import MdDay

# 初始化数据库连接
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=True)
engine_quote = create_engine('mysql+pymysql://qcy:qcy@192.168.39.65:3306/md_stock?charset=utf8', echo=True)

# TODO：修改回测起始日期
begin_yyyymmdd = '20170101'

# 所有交易日
sql = select([ChinaEtfPchRedmList.trade_date]).where(ChinaEtfPchRedmList.etf_sec_code == '510050.SH').distinct(
    ChinaEtfPchRedmList.trade_date)
df = pd.read_sql(sql, engine)
trade_dates = df['trade_date'].tolist()

# 今日开盘的pos会用今天盘前公布的申赎清单
# 申赎清单上的成分股权重，是用上一个交易日的收盘价计算的权重
sql = select([ChinaEtfPrevWeight.sec_code,
              ChinaEtfPrevWeight.trade_date,
              ChinaEtfPrevWeight.weight]).where(
    ChinaEtfPrevWeight.trade_date >= begin_yyyymmdd)
origin_pos_df = pd.read_sql(sql, engine)

all_sec_codes = list(set(origin_pos_df['sec_code'].tolist()))
all_sec_codes_six_digit = [x[:6] for x in all_sec_codes]

pos_df = origin_pos_df.pivot(index='trade_date', columns='sec_code', values='weight')

N1 = 5
N2 = 5
k1 = 0.5
k2 = 0.5

# random.randint(a, b) # [a, b]
# random.randrange(0, 50)  # [a, b)

# 按天循环
one_day_pos_list = []
for i in range(len(pos_df)):
    one_day_pos = pos_df.iloc[i, :].copy().dropna()
    over_bought_idxs = [random.randrange(0, 50) for _ in range(N1)]
    over_sold_idxs = [random.randrange(0, 50) for _ in range(N2)]

    one_day_pos[over_bought_idxs] *= (1 + k1)
    one_day_pos[over_sold_idxs] *= (1 - k2)

    one_day_pos_list.append(pd.DataFrame(one_day_pos).T)

real_pos_df = pd.concat(one_day_pos_list)
real_pos_df = real_pos_df.divide(real_pos_df.sum(axis=1), axis=0)  # 每天pos的weight归一

# 每日涨跌幅
sql = select([MdDay.stock, MdDay.d_day, MdDay.close, MdDay.adj_factor]).where(
    and_(MdDay.stock.in_(all_sec_codes_six_digit), MdDay.d_day >= begin_yyyymmdd))
px_df = pd.read_sql(sql, engine_quote)
px_df['adj_close'] = px_df['close'] * px_df['adj_factor']
px_df = px_df.pivot(columns='stock', index='d_day', values='adj_close')
ret_df = px_df.pct_change()
ret_df.columns = real_pos_df.columns

daily_ret = (real_pos_df * ret_df).sum(axis=1)
nav_df = (1 + daily_ret).cumprod()
