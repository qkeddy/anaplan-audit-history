# ===============================================================================
# Created:        3 Feb 2023
# Updated:
# @author:        Quinlan Eddy
# Description:    Module for generic Python operations
# ===============================================================================


import os
import sys
import logging
import time
import argparse
import json

# === Clear Console ===
def clear_console():
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


# === Read in configuration ===
def read_configuration_settings():
    try:
        with open("settings.json", "r") as settings_file:
            settings = json.load(settings_file)
        logging.info("Configuration read in successfully")
        return settings

    except:
        print("Unable to open the `settings.json` file. Please ensure the file is in the path of this Python module")
        # Exit with a non-zero exit code
        sys.exit(1)


# === Read CLI Arguments ===
def read_cli_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--register', action='store_true',
                        help="OAuth device registration")
    parser.add_argument('-c', '--client_id', action='store',
                        type=str, help="OAuth Client ID")
    args = parser.parse_args()
    return args
