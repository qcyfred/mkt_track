# coding: utf-8
from sqlalchemy import create_engine
import pandas as pd
import xlwings as xw
from sqlalchemy import (select)
from mkt_track_models import (AShareTalib,
                              AShareTablibComment)
from WindPy import w
import datetime

w.start()

# 初始化数据库连接:
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=True)

now_dt = datetime.datetime.now()
today_date = now_dt.date()
today_yyyymmdd = now_dt.strftime('%Y%m%d')

last_date_in_wind = (w.tdaysoffset(-1, today_date).Data[0][0]).date()  # 可以获取到的最新数据的交易日
last_date_yyyymmdd = last_date_in_wind.strftime('%Y%m%d')


# xb = xw.Book(r'd:\跟踪_多sheet.xlsx')

# sql = select([clazz]).order_by(clazz.sec_code)
# df = pd.read_sql(sql, engine, index_col='sec_code')
# sht = xb.sheets[sht_name]
# rng = sht.range('A1').expand()
# rng.value = None
# rng.value = df

sql = select([AShareTalib]).order_by(AShareTalib.sec_code)
df1 = pd.read_sql(sql, engine, index_col='sec_code')

sql = select([AShareTablibComment]).order_by(AShareTablibComment.sec_code)
df2 = pd.read_sql(sql, engine, index_col='sec_code')

df = df1.merge(df2, on='sec_code', how='left')
# print(df)
# df.to_clipboard()

# xb.save()
