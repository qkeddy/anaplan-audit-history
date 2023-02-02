# ===============================================================================
# Created:        2 Feb 2023
# @author:        Quinlan Eddy (Anaplan, Inc)
# Description:    Module for Anaplan OAuth2 Authentication
# ===============================================================================

import logging
import requests
import json
import AuthToken

# Enable Logging
logger = logging.getLogger(__name__)

# ===  Step #1 - Device grant   ===
# Response returns a Device ID and Verification URL


def get_device_id(oauth_client_id):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    get_body = {
        "client_id": oauth_client_id,
        "scope": "openid profile email offline_access"
    }

    res = requests.post('https://us1a.app.anaplan.com/oauth/device/code',
                        json=get_body, headers=get_headers)
    logging.info("Requesting OAuth Device Code")

    # Convert payload to dictionary for parsing
    j_res = json.loads(res.text)

    # Set values
    device_code = j_res['device_code']
    logging.info("Device Code received")

    # Pause for user authentication
    print('Please authenticate with Anaplan using this URL using an incognito browser: ',
          j_res['verification_uri_complete'])
    input("Press Enter to continue...")
    logging.info("Device authenticated")


# ===  Step #2 - Device grant   ===
# Response returns a `access_token` and `refresh_token`
def refresh_access_refresh_token(oauth_client_id, device_code):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    get_body = {
        "client_id": oauth_client_id,
        "device_code": device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code"
    }
    res = requests.post('https://us1a.app.anaplan.com/oauth/token',
                        json=get_body, headers=get_headers)
    logging.info("Requesting OAuth Access Token and Refresh Token")

    # Convert payload to dictionary for parsing
    j_res = json.loads(res.text)

    # Set values in AuthToken Dataclass
    AuthToken.Auth.access_token = j_res['access_token']
    AuthToken.Auth.refresh_token = j_res['refresh_token']
    logging.info("Access Token and Refresh Token received")

    # Write values to file system
    get_auth = {
        "access_token": AuthToken.Auth.access_token,
        "refresh_token": AuthToken.Auth.refresh_token
    }

    with open("auth.json", "w") as auth_file:
        json.dump(get_auth, auth_file)
        logging.info("Access Token and Refresh written to file system")
