# coding: utf-8
from sqlalchemy import Column, Date, Float, ForeignKey, Index, String
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import INTEGER
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
