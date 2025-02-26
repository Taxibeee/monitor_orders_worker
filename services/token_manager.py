import time
import requests

# Constants for the Bolt API
TOKEN_URL = 'https://oidc.bolt.eu/token'
headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
}

CLIENT_ID = 'TUQbP5BGNHCLtAzzteYKa'
CLIENT_SECRET = 'PFv8NsuSdyUdV_tE73nJr-3vuInPDt18yfvpygA6K9O6yPTKLJUBZvvqbFWAVQHUk6IYlq1mwOLcfNKfSRfftw'
GRANT_TYPE = 'client_credentials'
SCOPE = 'fleet-integration:api'

access_token = None
token_expiry = 0

data = {
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'grant_type': GRANT_TYPE,
    'scope': SCOPE
}
def fetch_access_token():
    global access_token, token_expiry

    try:
        response = requests.post(TOKEN_URL, headers=headers, data=data)
     

        token_data = response.json()
        access_token = token_data["access_token"]
        token_expiry = time.time() + token_data["expires_in"] - 60 # Refresh one minute early
    except requests.RequestException as e:
        raise Exception(f"Failed to fetch access token: {e}")
    

def get_access_token():
    global access_token, token_expiry

    if access_token is None or time.time() > token_expiry:
        fetch_access_token()
    return access_token