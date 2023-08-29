# ===============================================================================
# Description:    Module for Anaplan Basic & Cert Authentication
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
import globals
import base64

# Enable logger
logger = logging.getLogger(__name__)

# Forward SQLite logs to the logging module
apsw.ext.log_sqlite()

# ===  Login to Anaplan - Basic Auth  ===
# Login into Anaplan with basic authentication
def basic_authentication(uri, username, password):
    # Encode credentials
    encoded_credentials = str(base64.b64encode((f'{username}:{password}'
                                                ).encode('utf-8')).decode('utf-8'))
    # Set headers
    headers = {
        'Authorization': 'Basic ' + encoded_credentials,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    try:
        logger.info("Trying to log into Anaplan using Basic Authentication")
        print("Trying to log into Anaplan using Basic Authentication")
        res = anaplan_api(uri=uri, headers=headers)

        # Set values in AuthToken Dataclass
        globals.Auth.access_token = res['tokenInfo']['tokenValue']
        # globals.Auth.refresh_token = res['tokenInfo']['refreshTokenId']    # Not used
        logger.info("Access Token and Refresh Token received")
        print("Access Token and Refresh Token received")

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# ===  Fetch new Access Token  ===
# Response returns an updated `access_token` and `refresh_token`
def refresh_tokens(uri, delay):

    # If delay is set then pause
    if delay > 0:
        time.sleep(delay)

    # As this is a daemon thread, keep looping until main thread ends
    while True:

        # Set headers
        headers = {
            'Authorization': 'AnaplanAuthToken ' + globals.Auth.access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        try:
            logger.info("Requesting new Token")
            print("Requesting new Token")
            res = anaplan_api(uri=uri, headers=headers)

            # Set new Access Token
            globals.Auth.access_token = res['tokenInfo']['tokenValue']

            logger.info("Updated Access Token received")
            print("Updated Access Token received")

            # If delay is set then continue to refresh the token
            if delay > 0:
                time.sleep(delay)
            else:
                break

        except Exception as err:
            print(f'{err} in function "{sys._getframe().f_code.co_name}"')
            logging.error(
                f'{err} in function "{sys._getframe().f_code.co_name}"')
            sys.exit(1)

# ===  Refresh token class  ===
# Pass in values to be used with the refresh_tokens function
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

# === Interface with Anaplan REST API   ===
def anaplan_api(uri, headers={}, body={}):

    res = None

    try:
        # POST to the Anaplan REST API to authentication tokens
        res = requests.post(uri, headers=headers, json=body)

        # Check for unfavorable status codes
        res.raise_for_status()

        # Return a converted payload to a dictionary for direct parsing
        return json.loads(res.text)

    except requests.exceptions.HTTPError as err:
        print(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
        logging.error(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
