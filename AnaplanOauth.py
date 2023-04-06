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
import Globals



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
        "client_id": Globals.Auth.client_id,
        "scope": "openid profile email offline_access"
    }
    res = None

    try:
        logger.info("Requesting Device ID and Verification URL")
        res = requests.post(uri, headers=get_headers, json=get_body)

        # Convert payload to dictionary for parsing
        j_res = json.loads(res.text)

        # Check for unfavorable status codes
        res.raise_for_status()

        # Set values
        Globals.Auth.device_code = j_res['device_code']
        logger.info("Device Code successfully received")

        # Pause for user authentication
        print('Please authenticate with Anaplan using this URL using an incognito browser: ',
              j_res['verification_uri_complete'])
        input("Press Enter to continue...")
    
    except requests.exceptions.HTTPError as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)



# ===  Step #2 - Device grant   ===
# Response returns a `access_token` and `refresh_token`
def get_tokens(uri, database):
    # Set Headers
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    # Set Body
    get_body = {
        "client_id": Globals.Auth.client_id,
        "device_code": Globals.Auth.device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code"
    }
    res = None

    try:
        logger.info("Requesting OAuth Access Token and Refresh Token")
        res = requests.post(uri, headers=get_headers, json=get_body)

        # Check for unfavorable status codes
        res.raise_for_status()

        # Convert payload to dictionary for parsing
        j_res = json.loads(res.text)

        # Set values in AuthToken Dataclass
        Globals.Auth.access_token = j_res['access_token']
        Globals.Auth.refresh_token = j_res['refresh_token']
        logger.info("Access Token and Refresh Token received")

        # Persist token values
        write_token_db(database)

    except requests.exceptions.HTTPError as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)



# ===  Step #3 - Device grant  ===
# Response returns an updated `access_token` and `refresh_token`
def refresh_tokens(uri, database, delay):
    # If the refresh_token is not available then read from `auth.json`
    if Globals.Auth.refresh_token == "none":
        tokens = read_token_db(database)

        if tokens['client_id'] == "empty":
            logger.warning("This client needs to be authorized by Anaplan. Please run this script again with the following arguments: python3 anaplan.py -r -c <<enter Client ID>>. For more information, use the argument `-h`.")
            print("This client needs to be authorized by Anaplan. Please run this script again with the following arguments: python3 anaplan.py -r -c <<enter Client ID>>. For more information, use the argument `-h`.")
            # Exit with return code 1
            sys.exit(1)

        Globals.Auth.client_id = tokens['client_id']
        Globals.Auth.refresh_token = tokens['refresh_token']

    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
    }

    # If delay is set then pause 
    if delay > 0:
        time.sleep(delay)

    # As this is a daemon thread, keep looping until main thread ends
    while True:
        get_body = {
            "client_id": Globals.Auth.client_id,
            "refresh_token": Globals.Auth.refresh_token,
            "grant_type": "refresh_token"
        }
        res = None
        try:
            logger.info("Requesting a new OAuth Access Token and Refresh Token")
            print("Requesting a new OAuth Access Token and Refresh Token")
            res = requests.post(uri, headers=get_headers, json=get_body)

            # Check for unfavorable status codes
            res.raise_for_status()

            # Convert payload to dictionary for parsing
            j_res = json.loads(res.text)

            # Set values in AuthToken Dataclass
            Globals.Auth.access_token = j_res['access_token']
            Globals.Auth.refresh_token = j_res['refresh_token']
            logger.info("Updated Access Token and Refresh Token received")

            # Persist token values
            write_token_db(database=database)

            # If delay is set then continue to refresh the token
            if delay > 0:
                time.sleep(delay)
            else:
                break
        
        except requests.exceptions.HTTPError as err:
            print(f'{err} in function "{sys._getframe().f_code.co_name}"')
            logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
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
   def __init__(self, thread_id, name, delay, database, uri):
      print('Refresh Token', thread_id, uri)
      threading.Thread.__init__(self)
      self.thread_id = thread_id
      self.name = name
      self.delay = delay
      self.uri = uri
      self.database = database
      self.daemon = True

   # Overriding the default subfunction `run()`
   def run(self):
      # Initiate the thread
      print("Starting " + self.name)
      refresh_tokens(self.uri, self.database, self.delay)
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
        {"refresh_token": Globals.Auth.refresh_token}, Globals.Auth.client_id, algorithm="HS256")
    values = (Globals.Auth.client_id, encoded_token)

    # Check if SQLite database exists
    if os.path.isfile(database):
        # Create connection to the existing database
        connection = apsw.Connection(
            database, flags=apsw.SQLITE_OPEN_READWRITE)
        connection.execute("update anaplan set client_id=$client_id, refresh_token=$refresh_token", values)
    else:
        # Create a new database
        connection = apsw.Connection(database)
        connection.execute("create table if not exists anaplan (client_id, refresh_token)")
        connection.execute("insert into anaplan values($client_id, $refresh_token)", values)

    logger.info("Tokens updated")
