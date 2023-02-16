# ===============================================================================
# Description:    Module for Anaplan OAuth2 Authentication
# ===============================================================================

import sys
import os
import logging
import requests
import json
import time
import threading
import apsw
import apsw.ext
import jwt
import AuthToken


# Enable logger
logger = logging.getLogger(__name__)

# Forward SQLite logs to the logging module
apsw.ext.log_sqlite()


# ===  Step #1 - Device grant   ===
# Upon success, returns a Device ID and Verification URL
def get_device_id(uri):
    # Set Headers
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    # Set Body
    get_body = {
        "client_id": AuthToken.Auth.client_id,
        "scope": "openid profile email offline_access"
    }

    try:
        logger.info("Requesting Device ID and Verification URL")
        res = requests.post(uri, headers=get_headers, json=get_body)

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
        process_status_exceptions(res, uri)


# ===  Step #2 - Device grant   ===
# Response returns a `access_token` and `refresh_token`
def get_tokens(uri):
    # Set Headers
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    # Set Body
    get_body = {
        "client_id": AuthToken.Auth.client_id,
        "device_code": AuthToken.Auth.device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code"
    }

    try:
        logger.info("Requesting OAuth Access Token and Refresh Token")
        res = requests.post(uri, headers=get_headers, json=get_body)

        # Convert payload to dictionary for parsing
        j_res = json.loads(res.text)

        # Set values in AuthToken Dataclass
        AuthToken.Auth.access_token = j_res['access_token']
        AuthToken.Auth.refresh_token = j_res['refresh_token']
        logger.info("Access Token and Refresh Token received")

        # Persist token values
        write_token_db()

    except:
        # Check status codes
        process_status_exceptions(res, uri)


# ===  Step #3 - Device grant  ===
# Response returns an updated `access_token` and `refresh_token`
def refresh_tokens(uri, delay):
    # If the refresh_token is not available then read from `auth.json`
    if AuthToken.Auth.refresh_token == "none":
        tokens = read_token_db()

        if tokens['client_id'] == "empty":
            logger.warning("This client needs to be authorized by Anaplan. Please run this script again with the following arguments: python3 anaplan.py -r -c <<enter Client ID>>. For more information, use the argument `-h`.")
            print("This client needs to be authorized by Anaplan. Please run this script again with the following arguments: python3 anaplan.py -r -c <<enter Client ID>>. For more information, use the argument `-h`.")
            # Exit with return code 1
            sys.exit(1)

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
            logger.info(
                "Requesting a new OAuth Access Token and Refresh Token")
            print("Requesting a new OAuth Access Token and Refresh Token")
            res = requests.post(uri, headers=get_headers, json=get_body)

            # Convert payload to dictionary for parsing
            j_res = json.loads(res.text)

            # Set values in AuthToken Dataclass
            AuthToken.Auth.access_token = j_res['access_token']
            AuthToken.Auth.refresh_token = j_res['refresh_token']
            logger.info("Updated Access Token and Refresh Token received")

            # Persist token values
            write_token_db()


            # If delay is set than continue to refresh the token
            if delay > 0:
                time.sleep(delay)
            else:
                break
        except:
            # Check status codes
            process_status_exceptions(res, uri)
            logger.error("Error updating access and refresh tokens")
            print("Error updating access and refresh tokens")
            break


# ===  Refresh token class  ===
# Pass in values to be used with the refresh token function
# Explicitly set the thread to be a subordinate daemon that will stop processing with main thread
class refresh_token_thread (threading.Thread):
    # Overriding the default `__init__`
   def __init__(self, thread_id, name, delay, uri):
      print('Refresh Token', thread_id, uri)
      threading.Thread.__init__(self)
      self.thread_id = thread_id
      self.name = name
      self.delay = delay
      self.uri = uri
      self.daemon = True

   # Overriding the default subfunction `run()`
   def run(self):
      # Initiate the thread
      print("Starting " + self.name)
      refresh_tokens(self.uri, self.delay)
      print("Exiting " + self.name)



# === Process REST API endpoint exceptions ===
# Log exceptions to logger
def process_status_exceptions(res, uri):
    # Override linting
    # pyright: reportUnboundVariable=false

    if res.status_code == 401:
        logger.error('%s with URI: %s', json.loads(
            res.text)['error_description'], uri)
    elif res.status_code == 403:
        logger.error('%s with URI: %s', json.loads(
            res.text)['error_description'], uri)
    elif res.status_code == 404:
        logger.error('%s with URL: %s', json.loads(
            res.text)['message'], uri)
        logger.error('Please check device code or service URI')
        print('ERROR - Please check logs')


# === Read a SQLite database ===
def read_token_db():

    # Initialize variable
    tokens = {}

    # Check if SQLite database exists
    if os.path.isfile("token.db3"):
        # Create connection to the existing database
        connection = apsw.Connection(
            "token.db3", flags=apsw.SQLITE_OPEN_READONLY)

        # Get values
        for client_id, refresh_token in connection.execute("select client_id, refresh_token from anaplan"):
            tokens = {"client_id": client_id, "refresh_token": jwt.decode(
                refresh_token, client_id, algorithms=["HS256"])['refresh_token']}

    else:
        logger.warning("Database file does not exist")
        tokens = {"client_id": "empty", "refresh_token": "empty"}

    return tokens

# === Create or update a SQLite database ===
def write_token_db():

    # Encode
    encoded_token = jwt.encode(
        {"refresh_token": AuthToken.Auth.refresh_token}, AuthToken.Auth.client_id, algorithm="HS256")
    values = (AuthToken.Auth.client_id, encoded_token)

    # Check if SQLite database exists
    if os.path.isfile("token.db3"):
        # Create connection to the existing database
        connection = apsw.Connection(
            "token.db3", flags=apsw.SQLITE_OPEN_READWRITE)
        connection.execute("update anaplan set client_id=$client_id, refresh_token=$refresh_token", values)
    else:
        # Create a new database
        connection = apsw.Connection("token.db3")
        connection.execute("create table if not exists anaplan (client_id, refresh_token)")
        connection.execute("insert into anaplan values($client_id, $refresh_token)", values)

    logger.info("Tokens updated")
