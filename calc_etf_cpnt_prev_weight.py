# coding: utf-8
# TODO: 注意，目前，权重是根据昨日收盘价*今天早上的数量计算的，昨日收盘权重
from sqlalchemy import create_engine
from sqlalchemy import (select,
                        and_,
                        delete)
import pandas as pd
from mkt_track_models import (ChinaEtfPchRedmList,
                              ChinaEtfPrevWeight,
                              )
from md_stock_models import MdDay
import datetime

# 初始化数据库连接
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=False)
engine_quote = create_engine('mysql+pymysql://qcy:qcy@192.168.39.65:3306/md_stock?charset=utf8', echo=False)

today_date = datetime.datetime.now().date()
begin_date = datetime.datetime.now() + datetime.timedelta(days=-7)

# 所有交易日
sql = select([ChinaEtfPchRedmList.trade_date]).where(
    and_(ChinaEtfPchRedmList.etf_sec_code == '510050.SH',
         ChinaEtfPchRedmList.trade_date <= today_date,
         ChinaEtfPchRedmList.trade_date >= begin_date)).distinct(
    ChinaEtfPchRedmList.trade_date)
df = pd.read_sql(sql, engine)
trade_dates = df['trade_date'].tolist()

for i in range(1, len(trade_dates)):
    # for trade_date in [trade_dates[0]]:
    # 每天持股
    trade_date = trade_dates[i]
    prev_trade_date = trade_dates[i - 1]
    print(trade_date, prev_trade_date)
    sql = select([ChinaEtfPchRedmList.sec_code, ChinaEtfPchRedmList.volume]).where(
        ChinaEtfPchRedmList.trade_date == trade_date)
    df = pd.read_sql(sql, engine)
    df['six_digit_sec_code'] = df['sec_code'].apply(lambda x: x[:6])
    sec_codes = df['sec_code'].tolist()
    six_digit_sec_codes = df['six_digit_sec_code'].tolist()

    # 价格
    # 注意：申赎清单上的权重，应该是今天早上公布的数量 * 上一交易日的价格！
    sql = select([MdDay.stock, MdDay.close]).where(
        and_(MdDay.d_day == prev_trade_date, MdDay.stock.in_(six_digit_sec_codes)))
    px_df = pd.read_sql(sql, engine_quote)

    # 市值
    df = df.merge(px_df, left_on='six_digit_sec_code', right_on='stock', how='left')
    df = df[['sec_code', 'volume', 'close']]
    df['amount'] = df['volume'] * df['close']
    total_amount = df['amount'].sum()
    df['weight'] = df['amount'] / total_amount  # 注意：这个权重是每天早上能算出的权重，不是收盘的权重
    df['trade_date'] = trade_date
    df['etf_sec_code'] = '510050.SH'

    df_to_save = df[['sec_code', 'trade_date', 'etf_sec_code', 'weight']]

    # 先删除，再插入
    sql = delete(ChinaEtfPrevWeight).where(
        and_(ChinaEtfPrevWeight.trade_date == trade_date, ChinaEtfPrevWeight.sec_code.in_(sec_codes)))
    engine.execute(sql)
    df_to_save.to_sql(ChinaEtfPrevWeight.__tablename__, engine, index=False, if_exists='append')
