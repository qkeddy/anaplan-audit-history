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
import globals



# Enable logger
logger = logging.getLogger(__name__)

# Forward SQLite logs to the logging module
apsw.ext.log_sqlite()


# ===  Step #1 - Device grant - Get Verification URL  ===
# Upon success, returns a Device ID and Verification URL
def get_device_id(uri):

    # Set Body
    get_body = {
        "client_id": globals.Auth.client_id,
        "scope": "openid profile email offline_access"
    }

    try:
        logger.info("Requesting Device ID and Verification URL")
        print("Requesting Device ID and Verification URL")
        res = anaplan_api(uri=uri, body=get_body)

        # Set values
        globals.Auth.device_code = res['device_code']
        logger.info("Device Code successfully received")
        print("Device Code successfully received")

        # Pause for user authentication
        print('Please authenticate with Anaplan using this URL using an incognito browser: ',
              res['verification_uri_complete'])
        input("Press Enter to continue...")
    
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# ===  Step #2 - Device grant - Get Access & Refresh Token  ===
# Response returns a `access_token` and `refresh_token`
def get_tokens(uri, database):

    # Set Body
    get_body = {
        "client_id": globals.Auth.client_id,
        "device_code": globals.Auth.device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code"
    }
    res = None

    try:
        logger.info("Requesting OAuth Access Token and Refresh Token")
        print("Requesting OAuth Access Token and Refresh Token")
        res = anaplan_api(uri=uri, body=get_body)

        # Set values in AuthToken Dataclass
        globals.Auth.access_token = res['access_token']
        globals.Auth.refresh_token = res['refresh_token']
        logger.info("Access Token and Refresh Token received")
        print("Access Token and Refresh Token received")

        # Persist token values
        write_token_db(database)

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# ===  Step #3 - Device grant - Get new Access Token with Refresh Token  ===
# Response returns an updated `access_token` and `refresh_token`
def refresh_tokens(uri, database, delay, rotatable_token):

    # If the refresh_token is not available then read from from the token database
    if globals.Auth.refresh_token == "none":
        tokens = read_token_db(database)

        if tokens['client_id'] == "empty":
            logger.warning("This client needs to be authorized by Anaplan. Please run this script again with the following arguments: python3 main.py -r -c <<enter Client ID>>. For more information, use the argument `-h`.")
            print("This client needs to be authorized by Anaplan. Please run this script again with the following arguments: python3 main.py -r -c <<enter Client ID>>. For more information, use the argument `-h`.")
            
            # Exit with return code 1
            sys.exit(1)

        # Set values in AuthToken Dataclass
        globals.Auth.client_id = tokens['client_id']
        globals.Auth.refresh_token = tokens['refresh_token']

    # If delay is set then pause 
    if delay > 0:
        time.sleep(delay)

    # As this is a daemon thread, keep looping until main thread ends
    while True:
        get_body = {
            "client_id": globals.Auth.client_id,
            "refresh_token": globals.Auth.refresh_token,
            "grant_type": "refresh_token"
        }

        try:
            logger.info("Requesting new Token(s)")
            print("Requesting new Token(s)")
            res = anaplan_api(uri=uri, body=get_body)

            # Set new Access Token
            globals.Auth.access_token = res['access_token']

            # Set values in AuthToken Dataclass
            if rotatable_token:
                globals.Auth.refresh_token = res['refresh_token']
                logger.info("Updated Access Token and Refresh Token received")
                print("Updated Access Token and Refresh Token received")

                # Persist token values
                write_token_db(database=database)
            else:
                logger.info("Updated Access Token received")
                print("Updated Access Token received")

            # If delay is set then continue to refresh the token
            if delay > 0:
                time.sleep(delay)
            else:
                break
        
        except Exception as err:
            print(f'{err} in function "{sys._getframe().f_code.co_name}"')
            logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
            sys.exit(1)


# === Interface with Anaplan REST API   ===
def anaplan_api(uri, body={}):

    # Set Headers
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    res = None

    try:
        # POST to the Anaplan REST API to receive OAuth values
        res = requests.post(uri, headers=get_headers, json=body)

        # Check for unfavorable status codes
        res.raise_for_status()

        # Return a converted payload to a dictionary for direct parsing
        return json.loads(res.text)

    except requests.exceptions.HTTPError as err:
        print(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text} - check that `rotatableToken` is set properly in the `settings.json` file')
        logging.error(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text} - check that `rotatableToken` is set properly in the `settings.json` file')
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)

# ===  Refresh token class  ===
# Pass in values to be used with the refresh token function
# Explicitly set the thread to be a subordinate daemon that will stop processing with main thread
class refresh_token_thread (threading.Thread):
    # Overriding the default `__init__`
   def __init__(self, thread_id, name, delay, database, uri, rotatable_token):
      print('Refresh Token', thread_id, uri)
      threading.Thread.__init__(self)
      self.thread_id = thread_id
      self.name = name
      self.delay = delay
      self.database = database
      self.uri = uri
      self.rotatable_token = rotatable_token
      self.daemon = True

   # Overriding the default subfunction `run()`
   def run(self):
      # Initiate the thread
      print("Starting " + self.name)
      refresh_tokens(self.uri, self.database, self.delay, self.rotatable_token)
      print("Exiting " + self.name)


# === Read a SQLite database ===
def read_token_db(database):

    # Initialize variable
    tokens = {}

    # Check if SQLite database exists
    if os.path.isfile(database):
        # Create connection to the existing database
        connection = apsw.Connection(
            database, flags=apsw.SQLITE_OPEN_READONLY)

        # Get values
        for client_id, refresh_token in connection.execute("select client_id, refresh_token from anaplan"):
            tokens = {"client_id": client_id, "refresh_token": jwt.decode(
                refresh_token, client_id, algorithms=["HS256"])['refresh_token']}

    else:
        logger.warning("Database file does not exist")
        tokens = {"client_id": "empty", "refresh_token": "empty"}

    return tokens


# === Create or update a SQLite database ===
def write_token_db(database):

    # Encode
    encoded_token = jwt.encode(
        payload={"refresh_token": globals.Auth.refresh_token}, 
        key=globals.Auth.client_id, 
        algorithm="HS256")
    values = (globals.Auth.client_id, encoded_token)

    # Check if SQLite database exists
    if os.path.isfile(database):
        # Create connection to the existing database
        connection = apsw.Connection(
            database, flags=apsw.SQLITE_OPEN_READWRITE)
        
        # Pass to the SQL update statement the `client_id` and `refresh_token` stored in the values
        connection.execute("update anaplan set client_id=$client_id, refresh_token=$refresh_token", values)
    else:
        # Create a new database
        connection = apsw.Connection(database)

        # Create the database to store the encrypted tokens. 
        connection.execute("create table if not exists anaplan (client_id, refresh_token)")

        # Pass to the SQL insert statement the `client_id` and `refresh_token` stored in the values
        connection.execute("insert into anaplan values($client_id, $refresh_token)", values)

    logger.info("Tokens updated")
