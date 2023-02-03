# ===============================================================================
# Created:        2 Feb 2023
# @author:        Quinlan Eddy (Anaplan, Inc)
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import requests
import json
import sys
import os
import logging
import time

import threading
import time
import argparse

import AuthToken
import AnaplanOauth

# === Clear Console ===
if os.name == "nt":
	os.system("cls")
else:
	os.system("clear")
	

# === Setup Logger ===
# Dynamically set logfile name based upon current date.
log_file_path = "./"
local_time = time.strftime("%Y%m%d", time.localtime())
log_file = log_file_path + local_time + "-ANAPLAN-RUN.LOG"
log_file_level = logging.INFO  # Options: INFO, WARNING, DEBUG, INFO, ERROR, CRITICAL
logging.basicConfig(filename=log_file,
                    filemode='a',  # Append to Log
                    format='%(asctime)s  :  %(levelname)s  :  %(message)s',
                    level=log_file_level)



logging.info("************** Logger Started ****************")

# === Read in Arguments ===
parser = argparse.ArgumentParser()
parser.add_argument('-r', '--register', action='store_true',
                    help="OAuth device registration")
parser.add_argument('-c', '--client_id', action='store',
                    type=str, help="OAuth Client ID")


# ===  Set Variables ===
# Insert the OAuth2 Client ID
args = parser.parse_args()
register = args.register
oauth_client_id = args.client_id

if register:
	logging.info('Registering the device with Client ID: %s' % oauth_client_id)
	AnaplanOauth.get_device_id(
		oauth_client_id, 'https://us1a.app.anaplan.com/oauth/device/code')

sys.exit(0)


AuthToken.Auth.access_token = "test value"
AuthToken.Auth.refresh_token = "test value"

print("Finished")