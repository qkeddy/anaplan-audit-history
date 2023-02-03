# ===============================================================================
# Created:        2 Feb 2023
# @author:        Quinlan Eddy (Anaplan, Inc)
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import sys
import logging
import threading

import utils
import AnaplanOauth

# Clear the console
utils.clear_console()

# Enable logging
logger = logging.getLogger(__name__)

# Get configurations
settings = utils.read_configuration_settings()
args = utils.read_cli_arguments()

# Set configurations
device_id_uri = settings['get_device_id_uri']
tokens_uri = settings['get_tokens_uri']
register = args.register
oauth_client_id = args.client_id

# If register flag is set, then request the user to authenticate with Anaplan to create device code
if register:
	logger.info('Registering the device with Client ID: %s' % oauth_client_id)
	AnaplanOauth.get_device_id(oauth_client_id, device_id_uri)
	AnaplanOauth.get_tokens(oauth_client_id, tokens_uri)
else:
	print('Skipping device registration and refreshing the access_token')
	logger.info('Skipping device registration and refreshing the access_token')
	AnaplanOauth.refresh_tokens(oauth_client_id, tokens_uri)
	

	
	
print("Finished Processing")

sys.exit(0)


