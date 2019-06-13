#!/usr/bin/env python
# coding: utf-8
# TODO: 复权因子是自己手动下载的

import pandas as pd
from WindPy import w

w.start()

# 输入需要下载的年份，注意起止日期
year = 2019

fields = ['pe_ttm', 'fa_bps', 'close', 'pct_chg']

for field in fields:
    temp = w.wsd(
        "600000.SH,600016.SH,600019.SH,600028.SH,600029.SH,600030.SH,600036.SH,600048.SH,600050.SH,600104.SH,600196.SH,600276.SH,600309.SH,600340.SH,600519.SH,600547.SH,600585.SH,600606.SH,600690.SH,600703.SH,600887.SH,601006.SH,601088.SH,601138.SH,601166.SH,601169.SH,601186.SH,601211.SH,601229.SH,601288.SH,601318.SH,601328.SH,601336.SH,601360.SH,601390.SH,601398.SH,601601.SH,601628.SH,601668.SH,601688.SH,601766.SH,601800.SH,601818.SH,601857.SH,601888.SH,601939.SH,601988.SH,601989.SH,603259.SH,603993.SH",
        field, "{year}-01-01".format(year=year), "{year}-12-31".format(year=year), "")

    df = pd.DataFrame(temp.Data).T
    df.index = temp.Times
    df.columns = temp.Codes

    df.to_csv('{year}_{field}.csv'.format(year=year, field=field))
