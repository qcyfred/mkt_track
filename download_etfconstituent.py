#!/usr/bin/env python
# coding: utf-8
# 下载ETF申赎清单
import pandas as pd
from WindPy import w

w.start()

# TODO：修改日期
begin_yyyymmdd = '20180101'
end_yyyymmdd = '20181231'

temp = w.tdays(begin_yyyymmdd, end_yyyymmdd, "")
trade_dates = temp.Data[0]

for trade_date in trade_dates:
    today_yyyymmdd = trade_date.strftime('%Y%m%d')
    temp = w.wset("etfconstituent",
                  "date={today_yyyymmdd};windcode=510050.SH;"
                  "field=wind_code,volume,cash_substitution_mark,"
                  "cash_substitution_premium_ratio,fixed_substitution_amount".format(today_yyyymmdd=today_yyyymmdd))

    df = pd.DataFrame(temp.Data).T
    df.columns = temp.Fields
    df['trade_date'] = today_yyyymmdd
    df.to_csv('csv/etf/{today_yyyymmdd}.csv'.format(today_yyyymmdd=today_yyyymmdd), encoding='gb2312')
