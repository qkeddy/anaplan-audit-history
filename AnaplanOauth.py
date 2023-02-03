# ===============================================================================
# Created:        2 Feb 2023
# @author:        Quinlan Eddy (Anaplan, Inc)
# Description:    Module for Anaplan OAuth2 Authentication
# ===============================================================================

import logging
import requests
import json
import time
import AuthToken

# Enable Logging
logger = logging.getLogger(__name__)

# ===  Step #1 - Device grant   ===
# Upon success, returns a Device ID and Verification URL
def get_device_id(oauth_client_id, url):
    # Set Headers
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    # Set Body
    get_body = {
        "client_id": oauth_client_id,
        "scope": "openid profile email offline_access"
    }

    try:
        res = requests.post(url, json=get_body, headers=get_headers)
        # Convert payload to dictionary for parsing
        j_res = json.loads(res.text)
        # Set values
        device_code = j_res['device_code']
        logging.info("Device Code successfully received")
        # Pause for user authentication
        print('Please authenticate with Anaplan using this URL using an incognito browser: ',
              j_res['verification_uri_complete'])
        input("Press Enter to continue...")
    except:
        # Check status codes
        if res.status_code == 403:
            logging.error('%s with URI: %s', json.loads(res.text)['error_description'], url)
        elif res.status_code == 404:
            logging.error('%s with URL: %s', json.loads(res.text)['message'], url)
        logging.error('Please check device code or service URI')
        print('ERROR - Please check logs')


# ===  Step #2 - Device grant   ===
# Response returns a `access_token` and `refresh_token`
def get_tokens(oauth_client_id, device_code):
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

   # 2. OAuth: Device Grant Get Access and Refresh token
# ===  Step #3 - Device grant   ===
# Response returns an updated `access_token` and `refresh_token`


def refresh_tokens(oauth_client_id, refresh_token):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    # TODO Add loop

    while 0 == 0:
        get_body = {
            "grant_type": "refresh_token",
            "client_id": oauth_client_id,
            "refresh_token": refresh_token
        }

        # TODO add try / catch
        res = requests.post(
            'https://us1a.app.anaplan.com/oauth/token', json=get_body, headers=get_headers)
        logging.info("Requesting OAuth Access Token and Refresh Token")

        # Convert payload to dictionary for parsing
        j_res = json.loads(res.text)

        # Set values in AuthToken Dataclass
        AuthToken.Auth.access_token = j_res['access_token']
        AuthToken.Auth.refresh_token = j_res['refresh_token']
        logging.info("Updated Access Token and Refresh Token received")

        # Write values to file system
        get_auth = {
            "access_token": AuthToken.Auth.access_token,
            "refresh_token": AuthToken.Auth.refresh_token
        }

        with open("auth.json", "w") as auth_file:
            json.dump(get_auth, auth_file)
        logging.info("Updated Access Token and Refresh written to file system")
        time.sleep(5)
