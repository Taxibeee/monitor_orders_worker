from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Double
Base = declarative_base()

class OrderAnomaly(Base):
    __tablename__ = 'order_anomalies'  

    order_reference = Column(String(40), primary_key=True, index=True, nullable=False)
    driver_name = Column(String(255))
    driver_uuid = Column(String(40)) # Bolt driver
    payment_method = Column(String(40))
    order_status = Column(String(60))
    vehicle_model = Column(String(60))
    vehicle_license_plate = Column(String(20))
    terminal_name = Column(String(20)) # Corresponds to MyPOS transactions
    pickup_address = Column(String(255))
    ride_distance = Column(Integer)
    
    # Timestamp fields
    payment_confirmed_timestamp = Column(Integer)
    order_created_timestamp = Column(Integer)
    order_accepted_timestamp = Column(Integer)
    order_pickup_timestamp = Column(Integer)
    order_dropoff_timestamp = Column(Integer)
    order_finished_timestamp = Column(Integer)


    # Breakdown of fare
    ride_price = Column(Double)
    booking_fee = Column(Double)
    toll_fee = Column(Double)
    tip = Column(Double)
    cash_discount = Column(Double)
    commission = Column(Double)
    in_app_discount = Column(Double)
    net_earnings = Column(Double)
    cancellation_fee = Column(Double)
