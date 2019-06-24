# coding: utf-8
from sqlalchemy import create_engine
import pandas as pd
import xlwings as xw
from sqlalchemy import (select)
from mkt_track_models import (AShareBia,
                              AShareBiasQuantile,
                              AShareAlpha,
                              AShareAlphaQuantile,
                              ASharePeQuantile,
                              ASharePbQuantile,
                              AShareFinPit)
from WindPy import w
import datetime

w.start()

# 初始化数据库连接
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=True)

now_dt = datetime.datetime.now()
today_date = now_dt.date()
today_yyyymmdd = now_dt.strftime('%Y%m%d')

last_date_in_wind = (w.tdaysoffset(-1, today_date).Data[0][0]).date()  # 可以获取到的最新数据的交易日
last_date_yyyymmdd = last_date_in_wind.strftime('%Y%m%d')

# TODO: 需要更新etf申赎清单的权重
sheet_mapper = {
    '个股偏离度': AShareBia,
    '个股偏离度分位数': AShareBiasQuantile,
    '个股超额收益': AShareAlpha,
    '个股超额收益分位数': AShareAlphaQuantile,
    '个股财务指标': AShareFinPit,
    '个股PE分位数': ASharePeQuantile,
    '个股PB分位数': ASharePbQuantile,
}

xb = xw.Book(r'd:\跟踪_多sheet.xlsx')

for sht_name, clazz in sheet_mapper.items():
    sql = select([clazz]).where(clazz.trade_date.__eq__(last_date_in_wind)).order_by(clazz.sec_code)
    df = pd.read_sql(sql, engine, index_col='sec_code')
    sht = xb.sheets[sht_name]
    rng = sht.range('A1').expand()
    rng.value = None
    rng.value = df

xb.save()
xb.close()
