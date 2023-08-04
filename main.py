# ===============================================================================
# Description:    Main module for controlling flow execution
# ===============================================================================

import sys
import logging
import datetime
import pytz
import time


import utils
import anaplan_oauth
import globals
import anaplan_ops

# TODO - Add Model History
# TODO - Add option not to load to Anaplan
# TODO - Add ability to execute export actions of users in a particular model


def main():
	# Clear the console
	utils.clear_console()

	# Enable logging
	logger = logging.getLogger(__name__)

	# Get configurations from `settings.json` file
	settings = utils.read_configuration_settings()

	# Get and set current time stamp
	ts = datetime.datetime.now(pytz.timezone("US/Eastern"))
	globals.Timestamps.local_time_stamp = ts.strftime("%d-%m-%Y %H:%M:%S %Z")
	globals.Timestamps.gmt_epoch = str(int(time.time()))

	# Get configurations from the CLI
	args = utils.read_cli_arguments()
	register = args.register

	# Set SQLite database for token database
	token_db = f'{globals.Paths.databases}/token.db3'

	# Set OAuth Client ID and if the TTL is provided via the CLI, then override the default in the `dataclass`
	globals.Auth.client_id = args.client_id
	if args.token_ttl == "":
		globals.Auth.token_ttl = int(args.token_ttl)

	# If register flag is set, then request the user to authenticate with Anaplan to create device code
	if register:
		logger.info(
			f'Registering the device with Client ID: {globals.Auth.client_id}')
		anaplan_oauth.get_device_id(uri=f'{settings["uris"]["oauthService"]}/device/code')
		anaplan_oauth.get_tokens(uri=f'{settings["uris"]["oauthService"]}/token', database=token_db)

	else:
		print('Skipping device registration and refreshing the access_token')
		logger.info('Skipping device registration and refreshing the access_token')
		anaplan_oauth.refresh_tokens(
			uri=f'{settings["uris"]["oauthService"]}/token', 
			database=token_db, 
			delay=0,
			rotatable_token=settings['rotatableToken'])

	# Start background thread to refresh the `access_token`
	refresh_token = anaplan_oauth.refresh_token_thread(
		thread_id = 1, 
		name="Refresh Token", 
		delay=globals.Auth.token_ttl, 
		uri=f'{settings["uris"]["oauthService"]}/token', database=token_db,
		rotatable_token=settings['rotatableToken']
		)
	refresh_token.start()

	# Invoke functional Anaplan operations
	anaplan_ops.refresh_events(settings=settings)

	# Exit with return code 0
	sys.exit(0)



if __name__ == '__main__':
    main()
