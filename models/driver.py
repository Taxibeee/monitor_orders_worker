from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer


Base = declarative_base()
class DriverSQL(Base):
    __tablename__ = 'drivers'
    taxibee_id = Column(Integer, primary_key=True, index=True)
    bolt_driver_uuid = Column(String(40))
    bolt_partner_uuid = Column(String(40))
    chauffeurskaartnr = Column(String(25))
    phone = Column(String(15))
    email = Column(String(50))
    exact_debnr = Column(String(10))
    state = Column(String(25))
    mypos_operator_code = Column(String(10))
    full_name = Column(String(255))
    company_id = Column(String(25))
    inactivity_reason = Column(String(255))
    today_terminal_name = Column(String(20), nullable=True)