# coding: utf-8
from sqlalchemy import CHAR, Column, Date, Float, text
from sqlalchemy.dialects.mysql import BIGINT, BIT, TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class MdAvg(Base):
    __tablename__ = 'md_avg'

    stock = Column(CHAR(6), primary_key=True, nullable=False)
    d_day = Column(Date, primary_key=True, nullable=False, index=True)
    adj_factor = Column(Float)
    volume_avg = Column(Float(asdecimal=True))
    close_avg = Column(Float)
    avg_avg = Column(Float)
    volume_close_disc = Column(Float(asdecimal=True))
    close_avg_disc = Column(Float)
    volume_avg_disc = Column(Float(asdecimal=True))
    avg_avg_disc = Column(Float)
    volume_close_disc_x2 = Column(Float(asdecimal=True))
    close_avg_disc_x2 = Column(Float)


class MdDay(Base):
    __tablename__ = 'md_day'

    stock = Column(CHAR(6), primary_key=True, nullable=False)
    d_day = Column(Date, primary_key=True, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BIGINT(20))
    amount = Column(BIGINT(20))
    free_turn = Column(Float)
    adj_factor = Column(Float)
    is_suspension = Column(BIT(1))
    is_maxupordown = Column(TINYINT(4))
    is_st = Column(TINYINT(4), nullable=False, server_default=text("'0'"))
