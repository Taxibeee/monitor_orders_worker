from sqlalchemy import Column, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()  # Use the same Base as other models

class ExactDebnr(Base):
    __tablename__ = 'exact_debnr'
    
    driver_name = Column(String(100), nullable=False)
    bolt_driver_uuid = Column(String(80), nullable=False,primary_key=True)
    exact_debnr_number = Column(String(20), nullable=True) 
    ride_price_sum = Column(Float, nullable=False, default=0.0)
    commission_bolt = Column(Float, nullable=False, default=0.0)
    commission_tc = Column(Float, nullable=False, default=0.0)  # 25% of ride_price_sum
    tips_bolt = Column(Float, nullable=False, default=0.0)
    tips_mypos = Column(Float, nullable=False, default=0.0)  # Calculated later
    card_received = Column(Float, nullable=False, default=0.0)  # Calculated later
    cash_received = Column(Float, nullable=False, default=0.0)  # Calculated later
    card_terminal_value = Column(Float, nullable=False, default=0.0)  # Sum of card_received and cash_received