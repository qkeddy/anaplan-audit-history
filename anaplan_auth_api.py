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

from base64 import b64encode
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA512


# Enable logger
logger = logging.getLogger(__name__)

# Forward SQLite logs to the logging module
apsw.ext.log_sqlite()

# ===  Login to Anaplan - Basic Auth  ===
# Login into Anaplan with basic authentication
def basic_authentication(uri, username, password):
    # Encode credentials
    encoded_credentials = str(b64encode((f'{username}:{password}'
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


# ===  Login to Anaplan - Cert Auth  ===
# Login into Anaplan with Certificate authentication
def cert_authentication(uri, public_cert_path, private_key_path, private_key_passphrase=None):

    try:
        # Split the privateKeyPath string using ':' as a delimiter
        private_key_path_parts = private_key_path.split(':')

        # Assign the private key path and passphrase (if available)
        private_key_path = private_key_path_parts[0]
        private_key_passphrase = private_key_path_parts[1] if len(private_key_path_parts) > 1 else None

        # Open private key, import key using optional passphrase, and unpack signer for usage with encryption
        keyFile = open(private_key_path, 'r', encoding='utf-8')
        myKey = RSA.import_key(keyFile.read(), passphrase=private_key_passphrase)
        signer = pkcs1_15.new(myKey)

        # create random 100 byte message
        message_bytes = get_random_bytes(100)

        # UNENCRYPTED message b64encoded
        message_bytes_b64e = b64encode(message_bytes)
        message_str_b64e = message_bytes_b64e.decode('ascii')

        # ENCRYPTED message b64encoded
        message_hash = SHA512.new(message_bytes)
        message_hash_signed = signer.sign(message_hash)
        message_str_signed_b64e = b64encode(message_hash_signed).decode('utf-8')

        # Extract Public Certificate string
        pubic_cert = extract_certificate_string(public_cert_path)

        # Set headers
        headers = {
            'Authorization': 'CACertificate ' + pubic_cert,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        body = {
            'encodedData': message_str_b64e,
            'encodedSignedData': message_str_signed_b64e
        }

        logger.info("Trying to log into Anaplan using Certificate Authentication")
        print("Trying to log into Anaplan using Certificate Authentication")
        res = anaplan_api(uri=uri, headers=headers, body=body)

        # Set values in AuthToken Dataclass
        globals.Auth.access_token = res['tokenInfo']['tokenValue']
        # globals.Auth.refresh_token = res['tokenInfo']['refreshTokenId']    # Not used
        logger.info("Access Token and Refresh Token received")
        print("Access Token and Refresh Token received")
    
    except FileNotFoundError as file_err:
        print(f'Error: The Public Certificate or Private key file is not found: {file_err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'Error: The Public Certificate or Private key file is not found: {file_err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)

    except ValueError as value_err:
        print(f'Error: Invalid private key data: {value_err} in function "{sys._getframe().f_code.co_name}". Check the passphrase of the private key.')
        logging.error(f'Error: Invalid private key data: {value_err} in function "{sys._getframe().f_code.co_name}". Check the passphrase of the private key.')
        sys.exit(1)

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# Validate that the public certificate is in the valid PEM format
def is_valid_certificate_pem(pem_content):
    return "-----BEGIN CERTIFICATE-----" in pem_content and "-----END CERTIFICATE-----" in pem_content


# Validate that the private key is in the valid PEM format
def is_valid_private_key_pem(pem_content):
    return "-----BEGIN PRIVATE KEY-----" in pem_content and "-----END PRIVATE KEY-----" in pem_content

# Extract public certificate for use in the header
def extract_certificate_string(pem_file_path):
    with open(pem_file_path, 'r') as pem_file:
        pem_content = pem_file.read()
        if is_valid_certificate_pem(pem_content):
            lines = pem_content.strip().split("\n")
            # certificate_string = "".join(
            # line for line in lines if not line.startswith("-----"))
            certificate_lines = [
                line for line in lines if not line.startswith("-----")]
            certificate_string = ''.join(certificate_lines)
            return certificate_string
        else:
            logging.error(
                "Private or Public Key is not is not a proper PEM format. Please check the format and retry.")
            print(
                "Private or Public Key is not is not a proper PEM format. Please check the format and retry.")
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
