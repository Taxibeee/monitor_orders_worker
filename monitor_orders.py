from sqlalchemy.orm import Session
from database.db import get_engine
from models.in_progress_orders import InProgressOrder
from models.orders import Order
from models.order_anomalies import OrderAnomaly
import time
from services.token_manager import get_access_token
import requests
from datetime import datetime, timedelta

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
        
        current_time = datetime.now(utc=True)
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
            
            # Create a dictionary of Bolt orders for easy lookup
            bolt_orders_dict = {
                order['order_reference']: order['order_status'] 
                for order in bolt_orders
                if order.get('order_reference')
            }

            # Check each in-progress order
            for order in in_progress_orders:
                bolt_status = bolt_orders_dict.get(order.order_reference)

                # Check if order hasn't been updated in 2 hours
                if order.last_checked and order.last_checked < two_hours_ago:
                    print(f"Order {order.order_reference} hasn't been updated in 2 hours. Creating anomaly...")
                    # Create new anomaly record
                    anomaly = OrderAnomaly(**dict(order))
                    session.add(anomaly)
                    session.delete(order)
                    continue
                
                if bolt_status == "finished":
                    print(f"Order {order.order_reference} is finished. Updating status...")
                    # Update the order status in database
                    order.status = "finished"
                    new_order = Order(**dict(order))
                    session.add(new_order)
                    session.delete(order)
                elif bolt_status:
                    order.last_checked = datetime.now(utc=True)
                    print(f"Order {order.order_reference} status: {bolt_status}")
                else:
                    print(f"Order {order.order_reference} not found in Bolt API response")

            # Commit all changes
            session.commit()

        except requests.exceptions.RequestException as e:
            print(f"Error querying Bolt API: {e}")
            session.rollback()
        except Exception as e:
            print(f"Unexpected error: {e}")
            session.rollback()

if __name__ == "__main__":
    check_order_status()
