# ===============================================================================
# Description:    Main module for controlling flow execution
# ===============================================================================

import sys
import logging

import utils
import AnaplanOauth
import Globals
import AnaplanOps


def main():
	# Clear the console
	utils.clear_console()

	# Enable logging
	logger = logging.getLogger(__name__)

	# Get configurations from `settings.json` file
	settings = utils.read_configuration_settings()

	# Get configurations from the CLI
	args = utils.read_cli_arguments()
	register = args.register

	# Set SQLite database for token database
	token_db = f'{Globals.Paths.databases}/token.db3'

	# Set OAuth Client ID and if the TTL is provided via the CLI, then override the default in the `dataclass`
	Globals.Auth.client_id = args.client_id
	if args.token_ttl != "":
		Globals.Auth.token_ttl = int(args.token_ttl)

	# If register flag is set, then request the user to authenticate with Anaplan to create device code
	if register:
		logger.info(
			f'Registering the device with Client ID: {Globals.Auth.client_id}')
		AnaplanOauth.get_device_id(uri=settings['get_device_id_uri'])
		AnaplanOauth.get_tokens(uri=settings['get_tokens_uri'], database=token_db)

	else:
		print('Skipping device registration and refreshing the access_token')
		logger.info('Skipping device registration and refreshing the access_token')
		AnaplanOauth.refresh_tokens(
			uri=settings['get_tokens_uri'], database=token_db, delay=0)

	# Start background thread to refresh the `access_token`
	refresh_token = AnaplanOauth.refresh_token_thread(
		1, name="Refresh Token", delay=Globals.Auth.token_ttl, uri=settings['get_tokens_uri'], database=token_db)
	refresh_token.start()

	# Invoke functional Anaplan operations
	AnaplanOps.refresh_events(settings=settings)

	# Exit with return code 0
	sys.exit(0)



if __name__ == '__main__':
    main()
