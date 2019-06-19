# coding: utf-8
from sqlalchemy import create_engine
from sqlalchemy import (select)
import pandas as pd
import numpy as np
import talib
from mkt_track_utils import trans_number_to_float
from sqlalchemy.orm import sessionmaker
from mkt_track_models import (AShareTalib,
                              AShareTablibComment,
                              ASse50Description)

# 初始化数据库连接:
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=True)
engine_quote = create_engine('mysql+pymysql://qcy:qcy@192.168.39.65:3306/?charset=utf8', echo=True)
# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)


def trend_comment(x, c):
    if x < c:
        return '偏空'
    elif x > c:
        return '偏多'
    else:
        return '中性'


def trend_cond_comment(x, c):
    if x < c * (1 - 3e-4):
        return '偏空'
    elif x > c * (1 + 3e-4):
        return '偏多'
    else:
        return '中性'


def range_comment(x, d2, d1, u1, u2):
    if x > u2:
        return '超买'
    elif x > u1 and x <= u2:
        return '偏多'
    elif x > d1 and x <= u1:
        return '中性'
    elif x > d2 and x <= d1:
        return '偏空'
    else:
        return '超卖'


def adx_comment(x):
    if x > 20:
        return '偏多'
    elif x < -20:
        return '偏空'
    else:
        return '中性'


def get_sse50_stk_codes():
    sql = select([ASse50Description.sec_code])
    df = pd.read_sql(sql, engine)
    return df['sec_code'].tolist()


def calc_indicators(df):
    po = df.open.values
    ph = df.high.values
    pl = df.low.values
    pc = df.close.values

    po = np.array(po, dtype=float)
    ph = np.array(ph, dtype=float)
    pl = np.array(pl, dtype=float)
    pc = np.array(pc, dtype=float)

    ma = []
    ema = []
    for i in [5, 10, 20, 60, 120]:
        ma.append(talib.MA(pc, i)[-1])
        ema.append(talib.EMA(pc, i)[-1])

    macd = talib.MACD(pc)[2][-1]
    rsi = talib.RSI(pc, 14)[-1]
    stoch = talib.STOCH(ph, pl, pc, 9, 6)[0][-1]
    adx = talib.ADX(ph, pl, pc, 14)[-1]
    cci = talib.CCI(ph, pl, pc, 14)[-1]
    # willr = talib.WILLR(ph, pl, pc, 14)[-1]
    stochrsi = talib.STOCHRSI(pc, 14)[0][-1]
    uo = talib.ULTOSC(ph, pl, pc)[-1]
    roc = pc[-1] / pc[-2] - 1
    sar = talib.SAR(ph, pl)[-1]

    plusDI = talib.PLUS_DI(ph, pl, pc, 14)[-1]
    minusDI = talib.MINUS_DI(ph, pl, pc, 14)[-1]
    adx *= (lambda x: 1 if x > 0 else -1 if x < 0 else 0)(plusDI - minusDI)

    # print(ma, ema, macd, rsi, stoch, adx, cci, stochrsi, uo, roc, sar)
    indicators = {'ma': ma,
                  'ema': ema,
                  'macd': macd,
                  'rsi': rsi,
                  'stoch': stoch,
                  'adx': adx,
                  'cci': cci,
                  'stochrsi': stochrsi,
                  'uo': uo,
                  'roc': roc,
                  'sar': sar}
    return indicators


def save_indicators(indicators):
    session = DBSession()
    new_obj = AShareTalib()
    new_obj.sec_code = indicators['sec_code']
    new_obj.trade_date = indicators['trade_date']
    new_obj.freq = 'D'

    # ma, ema
    new_obj.ma5 = trans_number_to_float(indicators['ma'][0])
    new_obj.ma10 = trans_number_to_float(indicators['ma'][1])
    new_obj.ma20 = trans_number_to_float(indicators['ma'][2])
    new_obj.ma60 = trans_number_to_float(indicators['ma'][3])
    new_obj.ma120 = trans_number_to_float(indicators['ma'][4])

    new_obj.ema5 = trans_number_to_float(indicators['ema'][0])
    new_obj.ema10 = trans_number_to_float(indicators['ema'][1])
    new_obj.ema20 = trans_number_to_float(indicators['ema'][2])
    new_obj.ema60 = trans_number_to_float(indicators['ema'][3])
    new_obj.ema120 = trans_number_to_float(indicators['ema'][4])

    # macd, rsi, stoch, adx, cci, stochrsi, uo, roc, sar
    new_obj.macd = trans_number_to_float(indicators['macd'])
    new_obj.rsi = trans_number_to_float(indicators['rsi'])
    new_obj.cci = trans_number_to_float(indicators['cci'])
    new_obj.adx = trans_number_to_float(indicators['adx'])
    new_obj.stochrsi = trans_number_to_float(indicators['stochrsi'])
    new_obj.stoch = trans_number_to_float(indicators['stoch'])
    new_obj.uo = trans_number_to_float(indicators['uo'])
    new_obj.roc = trans_number_to_float(indicators['roc'])
    new_obj.sar = trans_number_to_float(indicators['sar'])

    session.add(new_obj)
    session.commit()
    session.close()


def calc_and_save_sse50_indicator():
    sse50_stk_codes = get_sse50_stk_codes()

    for sec_code in sse50_stk_codes:
        sec_code_jx = sec_code[:6]
        sql = """
        SELECT stock as sec_code, d_day as trade_date, 
        `open`,`high`,`low`,`close`,`adj_factor`
         FROM md_stock.md_day WHERE stock = '{sec_code_jx}' ORDER BY trade_date DESC limit 0, 500;
         """.format(sec_code_jx=sec_code_jx)
        df = pd.read_sql(sql, engine_quote)
        df['open'] = df['open'] * df['adj_factor'] / df['adj_factor'].values[-1]
        df['high'] = df['high'] * df['adj_factor'] / df['adj_factor'].values[-1]
        df['low'] = df['low'] * df['adj_factor'] / df['adj_factor'].values[-1]
        df['close'] = df['close'] * df['adj_factor'] / df['adj_factor'].values[-1]
        indicators = calc_indicators(df)
        indicators['sec_code'] = sec_code
        indicators['trade_date'] = df['trade_date'].values[-1]

        save_indicators(indicators)


def gen_comment(sec_code):
    sql = select([AShareTalib]).where(AShareTalib.sec_code == sec_code)
    df = pd.read_sql(sql, engine)
    indicators = df.T.to_dict()[0]
    six_digit_sec_code = sec_code[:6]
    sql = """SELECT `close` FROM md_stock.md_day WHERE stock = '{six_digit_sec_code}' order by d_day DESC limit 0, 1;""".format(
        six_digit_sec_code=six_digit_sec_code)
    px = engine_quote.execute(sql).fetchall()[0][0]

    comment_dict = dict()
    comment_dict['ma5'] = trend_cond_comment(px, indicators['ma5'])
    comment_dict['ma10'] = trend_cond_comment(px, indicators['ma10'])
    comment_dict['ma20'] = trend_cond_comment(px, indicators['ma20'])
    comment_dict['ma60'] = trend_cond_comment(px, indicators['ma60'])
    comment_dict['ma120'] = trend_cond_comment(px, indicators['ma120'])
    comment_dict['ema5'] = trend_cond_comment(px, indicators['ema5'])
    comment_dict['ema10'] = trend_cond_comment(px, indicators['ema10'])
    comment_dict['ema20'] = trend_cond_comment(px, indicators['ema20'])
    comment_dict['ema60'] = trend_cond_comment(px, indicators['ema60'])
    comment_dict['ema120'] = trend_cond_comment(px, indicators['ema120'])
    comment_dict['macd'] = trend_comment(indicators['macd'], 0)
    comment_dict['rsi'] = range_comment(indicators['rsi'], 30, 45, 55, 70)
    comment_dict['stoch'] = range_comment(indicators['stoch'], 20, 45, 55, 80)
    comment_dict['adx'] = adx_comment(indicators['adx'])
    comment_dict['cci'] = range_comment(indicators['cci'], -150, -50, 50, 150)
    comment_dict['stochrsi'] = range_comment(indicators['stochrsi'], 0.2, 0.45, 0.55, 0.8)
    comment_dict['uo'] = trend_comment(indicators['uo'], 50)
    comment_dict['roc'] = trend_comment(indicators['roc'], 0)
    comment_dict['sar'] = trend_comment(px, indicators['sar'])
    comment_dict['sec_code'] = sec_code
    comment_dict['trade_date'] = indicators['trade_date']
    return comment_dict


def save_comment(comment_dict):
    session = DBSession()
    new_obj = AShareTablibComment()
    new_obj.sec_code = comment_dict['sec_code']
    new_obj.trade_date = comment_dict['trade_date']
    new_obj.freq = 'D'

    new_obj.ma5 = comment_dict['ma5']
    new_obj.ma10 = comment_dict['ma10']
    new_obj.ma20 = comment_dict['ma20']
    new_obj.ma60 = comment_dict['ma60']
    new_obj.ma120 = comment_dict['ma120']
    new_obj.ema5 = comment_dict['ema5']
    new_obj.ema10 = comment_dict['ema10']
    new_obj.ema20 = comment_dict['ema20']
    new_obj.ema60 = comment_dict['ema60']
    new_obj.ema120 = comment_dict['ema120']
    new_obj.macd = comment_dict['macd']
    new_obj.rsi = comment_dict['rsi']
    new_obj.stoch = comment_dict['stoch']
    new_obj.adx = comment_dict['adx']
    new_obj.cci = comment_dict['cci']
    new_obj.stochrsi = comment_dict['stochrsi']
    new_obj.uo = comment_dict['uo']
    new_obj.roc = comment_dict['roc']
    new_obj.sar = comment_dict['sar']

    session.add(new_obj)
    session.commit()
    session.close()


def gen_and_save_sse50_comment():
    sse50_stk_codes = get_sse50_stk_codes()
    for sec_code in sse50_stk_codes:
        comment_dict = gen_comment(sec_code)
        save_comment(comment_dict)


calc_and_save_sse50_indicator()
gen_and_save_sse50_comment()
