# ===============================================================================
# Created:        2 Feb 2023
# @author:        Quinlan Eddy (Anaplan, Inc)
# Description:    Module for Anaplan OAuth2 Authentication
# ===============================================================================

import sys
import logging
import requests
import json
import time
import AuthToken


# Enable logger
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
        logger.info("Requesting Device ID and Verification URL")
        res = requests.post(url, headers=get_headers, json=get_body)

        # Convert payload to dictionary for parsing
        j_res = json.loads(res.text)

        # Set values
        AuthToken.Auth.device_code = j_res['device_code']
        logger.info("Device Code successfully received")
        
        # Pause for user authentication
        print('Please authenticate with Anaplan using this URL using an incognito browser: ',
              j_res['verification_uri_complete'])
        input("Press Enter to continue...")
    except:
        # Check status codes
        process_status_exceptions(res, url)



# ===  Step #2 - Device grant   ===
# Response returns a `access_token` and `refresh_token`
def get_tokens(oauth_client_id, url):
    # Set Headers
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    # Set Body
    get_body = {
        "client_id": oauth_client_id,
        "device_code": AuthToken.Auth.device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code"
    }

    try:
        logger.info("Requesting OAuth Access Token and Refresh Token")
        res = requests.post(url, headers=get_headers, json=get_body)
        
        # Convert payload to dictionary for parsing
        j_res = json.loads(res.text)

        # Set values in AuthToken Dataclass
        AuthToken.Auth.access_token = j_res['access_token']
        AuthToken.Auth.refresh_token = j_res['refresh_token']
        logger.info("Access Token and Refresh Token received")

        # Write values to file system
        get_auth = {
            "client_id": oauth_client_id,
            "access_token": AuthToken.Auth.access_token,
            "refresh_token": AuthToken.Auth.refresh_token
        }
        with open("auth.json", "w") as auth_file:
            json.dump(get_auth, auth_file)
            logger.info("Access Token and Refresh written to file system")

    except IOError:
        print('Unable to write file')
    
    except:
        # Check status codes
        process_status_exceptions(res, url)


# ===  Step #3 - Device grant  ===
# Response returns an updated `access_token` and `refresh_token`
def refresh_tokens(oauth_client_id, url):
    # If the refresh_token is not available then read from `auth.json`
    if AuthToken.Auth.refresh_token == "none":
        tokens = read_persisted_tokens()
        AuthToken.Auth.client_id = tokens['client_id']
        AuthToken.Auth.refresh_token = tokens['refresh_token']

    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    # As this is a daemon thread, keep looping until main thread ends
    while True:
        get_body = {
            "client_id": AuthToken.Auth.client_id,
            "refresh_token": AuthToken.Auth.refresh_token,
            "grant_type": "refresh_token"
        }

        try:
            logger.info("Requesting a new OAuth Access Token and Refresh Token")
            print("Requesting a new OAuth Access Token and Refresh Token")
            res = requests.post(url, headers=get_headers, json=get_body)
            
            # Convert payload to dictionary for parsing
            j_res = json.loads(res.text)

            # Set values in AuthToken Dataclass
            AuthToken.Auth.access_token = j_res['access_token']
            AuthToken.Auth.refresh_token = j_res['refresh_token']
            logger.info("Updated Access Token and Refresh Token received")

            # Write values to file system
            get_auth = {
                "client_id": AuthToken.Auth.client_id,
                "access_token": AuthToken.Auth.access_token,
                "refresh_token": AuthToken.Auth.refresh_token
            }

            with open("auth.json", "w") as auth_file:
                json.dump(get_auth, auth_file)
            logger.info("Updated Access Token and Refresh written to file system")
            print("Updated Access Token and Refresh written to file system")
            time.sleep(5)
        except:
            # Check status codes
            process_status_exceptions(res, url)
            logger.error("Error updating access and refresh tokens")
            print("Error updating access and refresh tokens")
            break


# === Read in configuration ===
def process_status_exceptions(res, url):
    # Override linting
    # pyright: reportUnboundVariable=false

    if res.status_code == 401:
        logger.error('%s with URI: %s', json.loads(
            res.text)['error_description'], url)
    elif res.status_code == 403:
        logger.error('%s with URI: %s', json.loads(
            res.text)['error_description'], url)
    elif res.status_code == 404:
        logger.error('%s with URL: %s', json.loads(
            res.text)['message'], url)
        logger.error('Please check device code or service URI')
        print('ERROR - Please check logs')


# === Read in configuration ===
def read_persisted_tokens():
    try:
        with open("auth.json", "r") as tokens_file:
            tokens = json.load(tokens_file)
        logger.info("Read in tokens successfully")
        return tokens

    except:
        print("Unable to open the `tokens.json` file. Please ensure the file is in the path of this Python module")
        logger.error(
            "Unable to open the `tokens.json` file. Please ensure the file is in the path of this Python module")
        # Exit with a non-zero exit code
        sys.exit(1)
