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
import anaplan_auth_api
import globals
import anaplan_ops

# TODO - Add Model History
# TODO - Add option not to load to Anaplan
# TODO - Add ability to execute export actions of users in a particular model to get visiting users
# TODO - Update README to clarify the Grant Type and length, improve docs settings.json doc, fix python libraries typo
# TODO - Update settings.json to use both Model and Workspace IDs


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

    # Based on authentication mode access Anaplan via the authentication API or OAuth API
    if settings["authenticationMode"] == "OAuth":  # Use OAuth
        print("Authorization via OAuth API")
        # Set OAuth Client ID and if the TTL is provided via the CLI, then override the default in the `dataclass`
        globals.Auth.client_id = args.client_id
        if args.token_ttl == "":
            globals.Auth.token_ttl = int(args.token_ttl)

        # If register flag is set, then request the user to authenticate with Anaplan to create device code
        if register:
            logger.info(
                f'Registering the device with Client ID: {globals.Auth.client_id}')
            anaplan_oauth.get_device_id(
                uri=f'{settings["uris"]["oauthService"]}/device/code')
            anaplan_oauth.get_tokens(
                uri=f'{settings["uris"]["oauthService"]}/token', database=token_db)

        else:
            print('Skipping device registration and refreshing the access_token')
            logger.info(
                'Skipping device registration and refreshing the access_token')
            anaplan_oauth.refresh_tokens(
                uri=f'{settings["uris"]["oauthService"]}/token',
                database=token_db,
                delay=0,
                rotatable_token=settings['rotatableToken'])

        # Start background thread to refresh the `access_token`
        refresh_token = anaplan_oauth.refresh_token_thread(
            thread_id=1,
            name="Refresh Token",
            delay=globals.Auth.token_ttl,
            uri=f'{settings["uris"]["oauthService"]}/token', database=token_db,
            rotatable_token=settings['rotatableToken']
        )
        refresh_token.start()
    else:   									# User Basic or Cert Auth
        # Set authentication base URI
        auth_uri = f'{settings["uris"]["authenticationApi"]}/authenticate'

        if settings["authenticationMode"] == "basic":
            print("Using Basic Authentication")
            # Set variables
            anaplan_auth_api.basic_authentication(
                uri=auth_uri, username=args.user, password=args.password)
        elif settings["authenticationMode"] == "cert_auth":
            print("Using Certificate Authentication")
            anaplan_auth_api.cert_authentication(
                uri=auth_uri, public_cert_path=settings["publicCertPath"], private_key_path=settings["privateKeyPath"], private_key_passphrase=args.
                private_key_passphrase)
        else:
            print("Please update the `settings.json` file with an authentication mode of `basic`, `cert_aut`, or `OAuth`")
            logging.error(
                "Please update the `settings.json` file with an authentication mode of `basic`, `cert_aut`, or `OAuth`")
            sys.exit(1)

        # Start background thread to refresh the `access_token`
        refresh_token = anaplan_auth_api.refresh_token_thread(
            thread_id=1,
            name="Refresh Token",
            delay=globals.Auth.token_ttl,
            uri=f'{settings["uris"]["authenticationApi"]}/refresh'
        )
        refresh_token.start()

    # Invoke functional Anaplan operations
    anaplan_ops.refresh_events(settings=settings)

    # Exit with return code 0
    sys.exit(0)



if __name__ == '__main__':
    main()
