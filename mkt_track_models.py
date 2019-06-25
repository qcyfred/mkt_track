# coding: utf-8
from sqlalchemy import Column, Date, Enum, Float, ForeignKey, Index, String, Table
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class AIndexDescription(Base):
    __tablename__ = 'a_index_description'

    sec_code = Column(String(32), primary_key=True, index=True)
    sec_name = Column(String(64), nullable=False)


class AShareDescription(Base):
    __tablename__ = 'a_share_description'

    sec_code = Column(String(32), primary_key=True)
    sec_name = Column(String(64), nullable=False)


class ASse50Description(Base):
    __tablename__ = 'a_sse50_description'

    sec_code = Column(String(32), primary_key=True)
    sec_name = Column(String(64))


class ChinaEtfPrevWeight(Base):
    __tablename__ = 'china_etf_prev_weight'

    sec_code = Column(String(32), primary_key=True, nullable=False)
    trade_date = Column(Date, primary_key=True, nullable=False)
    etf_sec_code = Column(String(32), primary_key=True, nullable=False)
    weight = Column(Float(asdecimal=True))


t_nav_df = Table(
    'nav_df', metadata,
    Column('sim_idx', INTEGER(11), index=True),
    Column('trade_date', Date),
    Column('nav', Float(asdecimal=True)),
    Column('params', String(128))
)


class AIndexBia(Base):
    __tablename__ = 'a_index_bias'

    sec_code = Column(ForeignKey('a_index_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False, index=True)
    value_20 = Column(Float(asdecimal=True))
    value_60 = Column(Float(asdecimal=True))
    value_120 = Column(Float(asdecimal=True))

    a_index_description = relationship('AIndexDescription')


class AIndexBiasQuantile(Base):
    __tablename__ = 'a_index_bias_quantile'

    sec_code = Column(ForeignKey('a_index_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False, index=True)
    observation_period = Column(INTEGER(11), primary_key=True, nullable=False)
    value_20 = Column(Float(asdecimal=True))
    value_60 = Column(Float(asdecimal=True))
    value_120 = Column(Float(asdecimal=True))

    a_index_description = relationship('AIndexDescription')


class AIndexEodPrice(Base):
    __tablename__ = 'a_index_eod_prices'
    __table_args__ = (
        Index('trade_date', 'trade_date', 'sec_code'),
    )

    sec_code = Column(ForeignKey('a_index_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False)
    close = Column(Float(asdecimal=True))
    pct_chg = Column(Float(asdecimal=True))

    a_index_description = relationship('AIndexDescription')


class AShareAlpha(Base):
    __tablename__ = 'a_share_alpha'
    __table_args__ = (
        Index('trade_date', 'trade_date', 'sec_code'),
    )

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False)
    value_20 = Column(Float(asdecimal=True))
    value_60 = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class AShareAlphaDd(Base):
    __tablename__ = 'a_share_alpha_dd'
    __table_args__ = (
        Index('trade_date', 'trade_date', 'sec_code'),
    )

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False)
    value_20 = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class AShareAlphaDdQuantile(Base):
    __tablename__ = 'a_share_alpha_dd_quantile'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False, index=True)
    observation_period = Column(INTEGER(11), primary_key=True, nullable=False)
    value_20 = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class AShareAlphaQuantile(Base):
    __tablename__ = 'a_share_alpha_quantile'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False, index=True)
    observation_period = Column(INTEGER(11), primary_key=True, nullable=False)
    value_20 = Column(Float(asdecimal=True))
    value_60 = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class AShareBia(Base):
    __tablename__ = 'a_share_bias'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False, index=True)
    value_20 = Column(Float(asdecimal=True))
    value_60 = Column(Float(asdecimal=True))
    value_120 = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class AShareBiasQuantile(Base):
    __tablename__ = 'a_share_bias_quantile'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False, index=True)
    observation_period = Column(INTEGER(11), primary_key=True, nullable=False)
    value_20 = Column(Float(asdecimal=True))
    value_60 = Column(Float(asdecimal=True))
    value_120 = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class AShareEodPrice(Base):
    __tablename__ = 'a_share_eod_prices'
    __table_args__ = (
        Index('trade_date', 'trade_date', 'sec_code'),
    )

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False)
    close = Column(Float(asdecimal=True))
    pct_chg = Column(Float(asdecimal=True))
    adjfactor = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class AShareFinPit(Base):
    __tablename__ = 'a_share_fin_pit'
    __table_args__ = (
        Index('trade_date', 'trade_date', 'sec_code'),
    )

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False)
    pe_ttm = Column(Float(asdecimal=True))
    fa_bps = Column(Float(asdecimal=True))
    pb = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class ASharePbQuantile(Base):
    __tablename__ = 'a_share_pb_quantile'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False, index=True)
    observation_period = Column(INTEGER(11), primary_key=True, nullable=False)
    val = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class ASharePeQuantile(Base):
    __tablename__ = 'a_share_pe_quantile'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False, index=True)
    trade_date = Column(Date, primary_key=True, nullable=False, index=True)
    observation_period = Column(INTEGER(11), primary_key=True, nullable=False)
    val = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class AShareTablibComment(Base):
    __tablename__ = 'a_share_tablib_comment'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False)
    trade_date = Column(Date, primary_key=True, nullable=False)
    freq = Column(Enum('D', 'W', 'M', 'H1', 'M1', 'M5', 'M10', 'M15', 'M30'), nullable=False)
    ma5 = Column(String(32))
    ma10 = Column(String(32))
    ma20 = Column(String(32))
    ma60 = Column(String(32))
    ma120 = Column(String(32))
    ema5 = Column(String(32))
    ema10 = Column(String(32))
    ema20 = Column(String(32))
    ema60 = Column(String(32))
    ema120 = Column(String(32))
    macd = Column(String(32))
    rsi = Column(String(32))
    stoch = Column(String(32))
    adx = Column(String(32))
    cci = Column(String(32))
    stochrsi = Column(String(32))
    uo = Column(String(32))
    roc = Column(String(32))
    sar = Column(String(32))

    a_share_description = relationship('AShareDescription')


class AShareTalib(Base):
    __tablename__ = 'a_share_talib'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False)
    trade_date = Column(Date, primary_key=True, nullable=False)
    freq = Column(Enum('D', 'W', 'M', 'H1', 'M1', 'M5', 'M10', 'M15', 'M30'), nullable=False)
    ma5 = Column(Float(asdecimal=True))
    ma10 = Column(Float(asdecimal=True))
    ma20 = Column(Float(asdecimal=True))
    ma60 = Column(Float(asdecimal=True))
    ma120 = Column(Float(asdecimal=True))
    ema5 = Column(Float(asdecimal=True))
    ema10 = Column(Float(asdecimal=True))
    ema20 = Column(Float(asdecimal=True))
    ema60 = Column(Float(asdecimal=True))
    ema120 = Column(Float(asdecimal=True))
    macd = Column(Float(asdecimal=True))
    rsi = Column(Float(asdecimal=True))
    stoch = Column(Float(asdecimal=True))
    adx = Column(Float(asdecimal=True))
    cci = Column(Float(asdecimal=True))
    stochrsi = Column(Float(asdecimal=True))
    uo = Column(Float(asdecimal=True))
    roc = Column(Float(asdecimal=True))
    sar = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')


class ChinaEtfPchRedmList(Base):
    __tablename__ = 'china_etf_pch_redm_list'

    sec_code = Column(ForeignKey('a_share_description.sec_code'), primary_key=True, nullable=False)
    trade_date = Column(Date, primary_key=True, nullable=False)
    etf_sec_code = Column(String(32), primary_key=True, nullable=False)
    volume = Column(INTEGER(11))
    cash_substitution_mark = Column(String(32))
    cash_substitution_premium_ratio = Column(Float(asdecimal=True))
    fixed_substitution_amount = Column(Float(asdecimal=True))

    a_share_description = relationship('AShareDescription')
