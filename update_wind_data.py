#!/usr/bin/env python
# coding: utf-8
"""
续传数据
最大日期T，从T+1开始，直到当前日期

更新这些表
a_index_eod_prices
a_share_eod_prices
a_share_fin_pit
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import (select,
                        delete)
from mkt_track_models import (AIndexDescription,
                              AShareDescription,
                              AIndexEodPrice,
                              AShareEodPrice,
                              AShareFinPit)
import pandas as pd
from WindPy import w
from sqlalchemy.sql import func
import datetime

w.start()

# 初始化数据库连接:
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=True)

# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)

now_dt = datetime.datetime.now()
today_date = now_dt.date()
today_yyyymmdd = now_dt.strftime('%Y%m%d')

last_date_in_wind = (w.tdaysoffset(-1, today_date).Data[0][0]).date()  # 可以获取到的最新数据的交易日


def get_trade_date(trade_date, offset):
    return (w.tdaysoffset(offset, trade_date).Data[0][0]).date()


# 指数日行情
def update_a_index_eod_prices():
    sql = select([AIndexDescription.sec_code])
    df = pd.read_sql(sql, engine)
    sec_codes = df['sec_code'].tolist()
    session = DBSession()

    for sec_code in sec_codes:
        # 查询数据库中保存的最大日期T
        last_day_in_db = session.query(func.max(AIndexEodPrice.trade_date)).one()[0]
        next_trade_date = get_trade_date(last_day_in_db, 1)

        # 如果需要更新的数据的日期不比可以获取到的最大日期大，才更新（有数据才更新）
        if next_trade_date <= last_date_in_wind:
            # 下wind的数据，多个指标，单个标的，很多天
            temp = w.wsd(sec_code, "close,pct_chg", next_trade_date, last_date_in_wind, "")
            df = pd.DataFrame(temp.Data).T
            df.index = temp.Times
            df.columns = temp.Fields
            df.reset_index(inplace=True)
            df.rename({'index': 'trade_date'}, axis=1, inplace=True)
            df['sec_code'] = sec_code

            delete(AIndexEodPrice).where(
                AIndexEodPrice.trade_date.between(next_trade_date, last_date_in_wind))  # 删掉，避免重复
            df.to_sql(AIndexEodPrice.__tablename__, engine, index=False, if_exists='append')  # 写入
        else:
            print('update_a_index_eod_prices: {sec_code} 无数据更新'.format(sec_code=sec_code))

    session.close()


# 更新个股日行情和pb、pe数据（因为没有pb，所以用p/b，在下载数据的时候就算好了）
def update_a_share_eod_prices_and_fin_pit():
    sql = select([AShareDescription.sec_code])
    df = pd.read_sql(sql, engine)
    sec_codes = df['sec_code'].tolist()
    session = DBSession()

    for sec_code in sec_codes:
        # 查询数据库中保存的最大日期T
        last_day_in_db = session.query(func.max(AShareFinPit.trade_date)).one()[0]
        next_trade_date = get_trade_date(last_day_in_db, 1)
        # 下wind的数据，多个指标，单个标的，很多天

        # 如果需要更新的数据的日期不比可以获取到的最大日期大，才更新（有数据才更新）
        if next_trade_date <= last_date_in_wind:
            temp = w.wsd(sec_code, "pe_ttm,fa_bps,close,pct_chg,adjfactor", next_trade_date, last_date_in_wind, "")
            df = pd.DataFrame(temp.Data).T
            df.index = temp.Times
            df.columns = temp.Fields
            df.reset_index(inplace=True)
            df.rename({'index': 'trade_date',
                       'CLOSE': 'close',
                       'PE_TTM': 'pe_ttm',
                       'FA_BPS': 'fa_bps',
                       'PCT_CHG': 'pct_chg',
                       'ADJFACTOR': 'adjfactor'}, axis=1, inplace=True)
            df['sec_code'] = sec_code

            df['pb'] = df['close'] / df['fa_bps']

            # 先删后插
            delete(AShareFinPit).where(AShareFinPit.trade_date.between(next_trade_date, last_date_in_wind))
            df[['sec_code', 'trade_date', 'pe_ttm', 'fa_bps', 'pb']].to_sql(AShareFinPit.__tablename__, engine,
                                                                            index=False, if_exists='append')  # 写入

            delete(AShareEodPrice).where(
                AShareEodPrice.trade_date.between(next_trade_date, last_date_in_wind))
            df[['sec_code', 'trade_date', 'close', 'pct_chg', 'adjfactor']].to_sql(AShareEodPrice.__tablename__,
                                                                                   engine,
                                                                                   index=False,
                                                                                   if_exists='append')  # 写入

        else:
            print('update_a_share_fin_pit: {sec_code} 无数据更新'.format(sec_code=sec_code))

    session.close()


update_a_index_eod_prices()  # 指数日行情
update_a_share_eod_prices_and_fin_pit()  # 更新个股日行情和pb、pe数据（因为没有pb，所以用p/b，在下载数据的时候就算好了）
