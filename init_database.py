# coding: utf-8
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from mkt_track_models import Base
from mkt_track_models import (AIndexDescription,
                              AShareDescription,
                              AIndexEodPrice,
                              AShareEodPrice,
                              AShareFinPit)
import pandas as pd
from mkt_track_utils import trans_number_type

# 初始化数据库连接:
engine = create_engine('mysql+pymysql://root:root@localhost:3306/db_mkt_track?charset=utf8', echo=True)

# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)


# 初始化股票info表
def init_a_share_description():
    df = pd.read_csv('csv/cpnt_stk_info.csv', encoding='gbk')
    df.rename({'wind_code': 'sec_code'}, axis=1, inplace=True)
    df = df[['sec_code', 'sec_name']]
    df.to_sql(AShareDescription.__tablename__, con=engine, index=False, if_exists='append')


# 初始化股票日行情
def init_a_share_eod_prices():
    session = DBSession()

    years = [2016, 2017, 2018, 2019]
    close_dfs = []
    pct_chg_dfs = []
    for year in years:
        close_df = pd.read_csv('csv/{year}_close.csv'.format(year=year), index_col='Unnamed: 0')
        close_dfs.append(close_df)
        pct_chg_df = pd.read_csv('csv/{year}_pct_chg.csv'.format(year=year), index_col='Unnamed: 0')
        pct_chg_dfs.append(pct_chg_df)

    close_df = pd.concat(close_dfs)
    pct_chg_df = pd.concat(pct_chg_dfs)

    adjfactor_df = pd.read_csv('csv/cpnt_adjfactor.csv', index_col='DateTime')
    sec_codes = close_df.columns
    close_df.index = pd.to_datetime(close_df.index)
    trade_dates = close_df.index.date
    close_mat = close_df.values
    pct_chg_mat = pct_chg_df.values
    adjfactor_mat = adjfactor_df.values

    for i in range(len(trade_dates)):
        for j in range(len(sec_codes)):
            trade_date = trade_dates[i]
            sec_code = sec_codes[j]

            new_obj = AShareEodPrice()
            new_obj.sec_code = sec_code
            new_obj.trade_date = trade_date
            new_obj.close = trans_number_type(close_mat[i, j])
            new_obj.pct_chg = trans_number_type(pct_chg_mat[i, j])
            new_obj.adjfactor = trans_number_type(adjfactor_mat[i, j])

            session.add(new_obj)

    session.commit()
    session.close()


# 初始化股票财务分析的pit数据
def init_a_share_fin_pit():
    session = DBSession()

    years = [2016, 2017, 2018, 2019]
    for year in years:

        fa_bps_df = pd.read_csv('csv/{year}_fa_bps.csv'.format(year=year), index_col='Unnamed: 0')
        pe_ttm_df = pd.read_csv('csv/{year}_pe_ttm.csv'.format(year=year), index_col='Unnamed: 0')
        close_df = pd.read_csv('csv/{year}_close.csv'.format(year=year), index_col='Unnamed: 0')
        pb_df = close_df / fa_bps_df

        sec_codes = close_df.columns
        close_df.index = pd.to_datetime(close_df.index)
        trade_dates = close_df.index.date
        pb_mat = pb_df.values
        pe_ttm_mat = pe_ttm_df.values
        fa_bps_mat = fa_bps_df.values

        for i in range(len(trade_dates)):
            for j in range(len(sec_codes)):
                trade_date = trade_dates[i]
                sec_code = sec_codes[j]

                new_obj = AShareFinPit()
                new_obj.sec_code = sec_code
                new_obj.trade_date = trade_date
                new_obj.fa_bps = trans_number_type(fa_bps_mat[i, j])
                new_obj.pe_ttm = trans_number_type(pe_ttm_mat[i, j])
                new_obj.pb_ttm = trans_number_type(pb_mat[i, j])

                session.add(new_obj)

        session.commit()
        session.close()


# 初始化指数info表
def init_a_index_description():
    session = DBSession()

    sec_codes = ['000016.SH', '000300.SH', '000905.SH']
    sec_names = ['上证50', '沪深300', '中证500']

    for sec_code, sec_name in zip(sec_codes, sec_names):
        new_obj = AIndexDescription()
        new_obj.sec_code = sec_code
        new_obj.sec_name = sec_name
        session.add(new_obj)

    session.commit()
    session.close()


# 初始化指数日行情
def init_a_index_eod_price():
    df = pd.read_csv('csv/sse50_mkt_data.csv')
    df['sec_code'] = '000016.SH'
    df.rename({'DateTime': 'trade_date'}, axis=1, inplace=True)
    df.to_sql(AIndexEodPrice.__tablename__, con=engine, index=False, if_exists='append')

    df = pd.read_csv('csv/csi300_mkt_data.csv')
    df['sec_code'] = '000300.SH'
    df.rename({'DateTime': 'trade_date'}, axis=1, inplace=True)
    df.to_sql(AIndexEodPrice.__tablename__, con=engine, index=False, if_exists='append')

    df = pd.read_csv('csv/csi500_mkt_data.csv')
    df['sec_code'] = '000905.SH'
    df.rename({'DateTime': 'trade_date'}, axis=1, inplace=True)
    df.to_sql(AIndexEodPrice.__tablename__, con=engine, index=False, if_exists='append')


Base.metadata.drop_all(engine)  # drop所有表
Base.metadata.create_all(engine)  # 创建表结构

init_a_share_description()  # 初始化股票info表
init_a_share_eod_prices()  # 初始化股票日行情
init_a_share_fin_pit()  # 初始化股票财务分析的pit数据

init_a_index_description()  # 初始化指数info表
init_a_index_eod_price()  # 初始化指数日行情
