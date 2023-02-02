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
import AuthToken
import threading
import time
import argparse


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



AuthToken.Auth.access_token = "test value"
AuthToken.Auth.refresh_token = "test value"
