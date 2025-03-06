from sqlalchemy.orm import Session
from database.db import get_engine
from sqlalchemy import inspect
from models.in_progress_orders import InProgressOrder
from models.orders import Order
from models.order_anomalies import OrderAnomaly
import time
from services.token_manager import get_access_token
import requests
from datetime import datetime, timedelta, timezone
from models.exact_debnr import ExactDebnr , Base as ExactDebnrBase
from models.driver import DriverSQL, Base as DriverBase
import csv
COMPANY_ID = "129914"
FLEET_ORDERS_URL = "https://node.bolt.eu/fleet-integration-gateway/fleetIntegration/v1/getFleetOrders"

def get_minimum_timestamp():
    engine = get_engine()
    with Session(engine) as session:
        # Get the minimum timestamp from the InProgressOrders table
        min_timestamp = session.query(InProgressOrder.order_created_timestamp).order_by(InProgressOrder.order_created_timestamp.asc()).first()

        if min_timestamp:
            # Subtract 24 hours from the minimum timestamp
            min_timestamp = min_timestamp[0] 
            return min_timestamp - 50 # Add buffer for being on the safe side
        else:
            # If no orders, return None
            print("No orders found in InProgressOrders table")
            return

def update_exact_debnr(session, order, driver=None):
    """
    Dedicated function to handle ExactDebnr record creation and updates. This ensures consistent handling of ride_prices and commissions.

    Args:
        session: SQLALCHEMY session
        order: Order object/dictionary with order details
        driver: Optional driver record if already retrieved
    """

    # Standard way to calculate prices - always accounting for discounts
    ride_price = float(order.get("ride_price", 0.0) or 0.0) - float(order.get("in_app_discount", 0.0) or 0.0)
    tips_bolt = float(order.get("tip", 0.0) or 0.0)
    commission_bolt = float(order.get("commission", 0.0) or 0.0)
    payment_method = order.get("payment_method", "")

    # load exact debnr mapping
    exact_debnr_mapping = load_exact_debnr_mapping() if not hasattr(update_exact_debnr, 'mapping') else update_exact_debnr.mapping
    update_exact_debnr.mapping = exact_debnr_mapping

    exact_debnr = session.query(ExactDebnr).filter_by(bolt_driver_uuid=order.driver_uuid).first()

    if not exact_debnr:
        if not driver:
            driver = session.query(DriverSQL).filter_by(bolt_driver_uuid=order.driver_uuid).first()

        if driver:
            # Create a new record with initial values
            exact_debnr = ExactDebnr(
                bolt_driver_uuid=order.driver_uuid,
                driver_name=driver.full_name if driver.full_name else "Unknown",
                exact_debnr_number=exact_debnr_mapping.get(order.driver_uuid),
                ride_price_sum=ride_price,
                commission_bolt=commission_bolt,
                commission_tc=ride_price * 0.25,
                tips_bolt=tips_bolt,
                tips_mypos=0.0,
                cash_received=0.0,
                card_received=0.0,
                card_terminal_value=ride_price if payment_method == "card_terminal" else 0.0
            )
            session.add(exact_debnr)
            return exact_debnr
        else:
            print(f"Warning: No driver record found for UUID {order.driver_uuid}. Financial data not recorded.")
            return None
    else:
        exact_debnr.ride_price_sum += ride_price
        exact_debnr.tips_bolt += tips_bolt
        exact_debnr.commission_bolt += commission_bolt

        exact_debnr.commission_tc += (ride_price * 0.25)
        
        if payment_method == "card_terminal":
            exact_debnr.card_terminal_value += ride_price
        
        return exact_debnr




def check_order_status():
    """
    Check status of orders in InProgressOrders table against Bolt API
    and update if they are finished
    """
    engine = get_engine()
    with Session(engine) as session:
        # Get all orders from InProgressOrders
        in_progress_orders = session.query(InProgressOrder).all()
        
        if not in_progress_orders:
            print("No orders to check")
            return
        
        exact_debnr_mapping = load_exact_debnr_mapping()
        current_time = datetime.now(timezone.utc)
        two_hours_ago = current_time - timedelta(hours=2)
        
        # Set time range for API query (last 24 hours to now)
        end_ts = int(time.time())
        start_ts = get_minimum_timestamp() # mininum timestamp in the in progress orders

        # Get orders from Bolt API
        token = get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        payload = {
            "offset": 0,
            "limit": 1000,
            "company_ids": [COMPANY_ID],
            "company_id": COMPANY_ID,
            "start_ts": start_ts,
            "end_ts": end_ts,
        }

        try:
            response = requests.post(FLEET_ORDERS_URL, json=payload, headers=headers)
            response.raise_for_status()  # Raise exception for bad status codes
            
            bolt_orders = response.json().get("data", {}).get("orders", [])


            # Flatten order_price
            for order in bolt_orders:
                if "order_price" in order:
                    order.update(order.pop("order_price"))


            
            # Create a dictionary of Bolt orders for easy lookup
            bolt_orders_dict = {
                order['order_reference']: (order['order_status'], order.get("ride_price")) 
                for order in bolt_orders
                if order.get('order_reference')
            }

            # Check each in-progress order
            for order in in_progress_orders:
                bolt_info = bolt_orders_dict.get(order.order_reference)

                # Check if order hasn't been updated in 2 hours
                if order.last_checked and order.last_checked < two_hours_ago:
                    print(f"Order {order.order_reference} hasn't been updated in 2 hours. Creating anomaly...")
                    # Create new anomaly record
                    anomaly = OrderAnomaly(**dict(order))
                    session.add(anomaly)
                    session.delete(order)
                    continue
                
                if not bolt_info:
                    print(f"Order {order.order_reference} not found in Bolt API response")
                    continue

                bolt_status, ride_price = bolt_info

                if bolt_status == "finished":
                    if ride_price is None or ride_price == 0:
                        order.last_checked = datetime.now(timezone.utc)
                        print(f"Order {order.order_reference} is finished but has no ride_price yet. Waiting for update...")
                    else:
                        print(f"Order {order.order_reference} is finished. Updating status...")
                        order.order_status = "finished"

                        exact_debnr = session.query(ExactDebnr).filter_by(bolt_driver_uuid=order.driver_uuid).first()
                        driver = session.query(DriverSQL).filter_by(bolt_driver_uuid=order.driver_uuid).first()

                        if not exact_debnr and driver:
                            ride_price = float(order.get("ride_price", 0.0) or 0.0) - float(order.get("in_app_discount", 0.0) or 0.0)
                            tips_bolt = float(order.get("tip", 0.0) or 0.0)  
                            commission_bolt = float(order.get("commission", 0.0) or 0.0)  # Adjust field name if different
                            payment_method = order.get("payment_method", "")
                            exact_debnr = ExactDebnr(
                                bolt_driver_uuid=order.driver_uuid,
                                driver_name=driver.full_name if driver.full_name else "Unknown",
                                exact_debnr_number=exact_debnr_mapping.get(order.driver_uuid),
                                ride_price_sum=ride_price,
                                commission_bolt= commission_bolt,
                                commission_tc=ride_price * 0.25,
                                tips_bolt=tips_bolt,
                                tips_mypos=0.0,
                                card_received=0.0,
                                cash_received=0.0,
                                card_terminal_value = ride_price if payment_method== "card_terminal" else 0.0
                            )
                            session.add(exact_debnr)

               
                
                        if exact_debnr:
                            # Extract values from order_data (assuming these fields exist in order_price)
                                ride_price = float(order.get("ride_price", 0.0) or 0.0) - float(order.get("in_app_discount", 0.0) or 0.0)
                                tips_bolt = float(order.get("tip", 0.0) or 0.0)  
                                commission_bolt = float(order.get("commission", 0.0) or 0.0)  # Adjust field name if different
                                payment_method = order.get("payment_method", "")
                            # Update sums
                                exact_debnr.ride_price_sum += ride_price
                                exact_debnr.tips_bolt += tips_bolt
                                exact_debnr.commission_bolt += commission_bolt
                                exact_debnr.commission_tc = exact_debnr.ride_price_sum * 0.25  # 25% of ride_price_sum
                                exact_debnr.tips_mypos = 0.0  # Always 0
                                if payment_method == "card_terminal":
                                    exact_debnr.card_terminal_value += ride_price

                        new_order = Order(**order)
                        session.add(new_order)
                        session.delete(order)
                elif bolt_status:
                    order.last_checked = datetime.now(timezone.utc)
                    print(f"Order {order.order_reference} is still in progress. Updating last_checked...")

            # Commit all changes
            session.commit()

        except requests.exceptions.RequestException as e:
            print(f"Error querying Bolt API: {e}")
            session.rollback()
        except Exception as e:
            print(f"Unexpected error: {e}")
            session.rollback()

def process_single_order(engine, order, bolt_orders_dict, two_hours_ago):
    """
    Process a single order in its own transaction

    Args:
        engine (SQLALCHEMY engine)
        order: InProgressOrder object to process
        bolt_orders_dict: Dictionary of Bolt orders from API
        two_hours_ago: Timestamp for checking order age
    """

    with Session(engine) as session:
        try:
            if order.last_checked = order.last_checked < two_hours_ago:
                print(f"Order {order.order_reference} hasn't been updated in 2 hours. Creating anomaly...")
                # Create new anomaly record
                anomaly = OrderAnomaly(**dict(order))
                session.add(anomaly)
                session.delete(order)
                session.commit()
                return
            
            # Check if order exists in Bolt API reposne
            bolt_info = bolt_orders_dict.get(order.order_reference)
            if not bolt_info:
                print(f"Order {order.order_reference} not found in Bolt API response")
                order.last_checked = datetime.now(timezone.utc)
                session.commit()
                return
            
            bolt_status, bolt_order = bolt_info

            if bolt_status == "finished":
                # Access ride_price from full order object
                if bolt_order.get("ride_price") is None or float(bolt_order.get("ride_price", 0.0) or 0.0) == 0:
                    order.last_checked = datetime.now(timezone.utc)
                    print(f"Order {order.order_reference} is finished but has no ride_price yet. Waiting for update...")
                    session.commit()
                    return
                
                print(f"Order {order.order_reference} is finished. Updating status...")
                order.order_status = "finished"

                # GEt driver record once
                driver = session.query(DriverSQL).filter_by(bolt_driver_uuid=order.driver_uuid).first()

                # update or create exact_debnr record using fucntion
                # We now pass the complete bolt_order which has all the details
                update_exact_debnr(session, bolt_order, driver)

                # Create completed order record
                new_order = Order(**vars(order))
                session.add(new_order)
                session.delete(order)
            else:
                order.last_checked = datetime.now(timezone.utc)
                print(f"Order {order.order_reference} is still in progress. Updating last_checked...")
            
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error processing order {order.order_reference} is still in progress. Updating last_checked... {e}")

def load_exact_debnr_mapping():
    try:
        mapping = {}
        with open('dim_drivers.csv', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                uuid = row.get('bolt_driver_uuid')
                debnr = row.get('exact_debnr')
                if uuid:
                    mapping[uuid] = debnr if debnr else None
        return mapping
    except FileNotFoundError:
        print("dim_drivers.csv not found. exact_debnr_number will be None.")
        return {}
    except Exception as e:
        print(f"Error loading dim_drivers.csv: {e}")
        return {}


def create_exact_debnr_table_if_not_exists():
    engine = get_engine()
    inspector = inspect(engine)
    if not inspector.has_table("exact_debnr"):
        ExactDebnrBase.metadata.create_all(engine)  # Use ExactDebnr's metadata
        print("ExactDebnr table created")
    else:
        print("ExactDebnr table already exists")

if __name__ == "__main__":
    create_exact_debnr_table_if_not_exists()      
    check_order_status()
