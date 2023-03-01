# ===============================================================================
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import sys
import logging

import utils
import AnaplanOauth
import AuthToken
import AnaplanOps

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
users_uri = settings['get_users_uri']
audit_events_uri = settings['get_audit_events_uri']
workspaces_uri = settings['get_workspaces_uri']
models_uri = settings['get_models_uri']
imports_uri = settings['get_imports_uri']
exports_uri = settings['get_exports_uri']
processes_uri = settings['get_processes_uri']
actions_uri = settings['get_actions_uri']
cloudworks_uri = settings['get_cloudworks_uri']
register = args.register
database_file = "audit.db3"
AuthToken.Auth.client_id = args.client_id
if args.token_ttl == "":
	AuthToken.Auth.token_ttl = int(args.token_ttl)

# If register flag is set, then request the user to authenticate with Anaplan to create device code
if register:
	logger.info(f'Registering the device with Client ID: {AuthToken.Auth.client_id}')
	AnaplanOauth.get_device_id(device_id_uri)
	AnaplanOauth.get_tokens(tokens_uri)

else:
	print('Skipping device registration and refreshing the access_token')
	logger.info('Skipping device registration and refreshing the access_token')
	AnaplanOauth.refresh_tokens(tokens_uri, 0)

# Start background thread to refresh the `access_token`
refresh_token = AnaplanOauth.refresh_token_thread(
	1, name="Refresh Token", delay=AuthToken.Auth.token_ttl, uri=tokens_uri)
refresh_token.start()

# Load User Activity Codes
AnaplanOps.get_usr_activity_codes(database_file=database_file)

# Get Users
AnaplanOps.get_users(users_uri, database_file)

# Get Workspaces
workspace_ids = AnaplanOps.get_anaplan_paged_data(uri=workspaces_uri, token_type="Bearer ", database_file=database_file,
                                  database_table="workspaces", record_path="workspaces", json_path=['meta', 'paging', 'next'])

# Get Models in all Workspace
for ws_id in workspace_ids:
	model_ids = AnaplanOps.get_anaplan_paged_data(uri=models_uri.replace('{{workspace_id}}', ws_id), token_type="Bearer ", database_file=database_file,
                                         database_table="models", record_path="models", json_path=['meta', 'paging', 'next'])

	# Loop through each Model to get details
	for mod_id in model_ids:
		# Get Import Actions in all Models in all Workspaces
		AnaplanOps.get_anaplan_paged_data(uri=imports_uri.replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
									database_table="actions", record_path="imports", json_path=['meta', 'paging', 'next'], workspace_id=ws_id, model_id=mod_id)

		# Get Import Actions in all Models in all Workspaces
		AnaplanOps.get_anaplan_paged_data(uri=exports_uri.replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                    database_table="actions", record_path="exports", json_path=['meta', 'paging', 'next'], workspace_id=ws_id, model_id=mod_id)

		# Get Import Processes in all Models in all Workspaces
		AnaplanOps.get_anaplan_paged_data(uri=processes_uri.replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                    database_table="actions", record_path="processes", json_path=['meta', 'paging', 'next'], workspace_id=ws_id, model_id=mod_id)

# Get CloudWorks Integrations
# TODO - adjust paging as this endpoint uses a different approach
AnaplanOps.get_anaplan_paged_data(uri=cloudworks_uri, token_type="AnaplanAuthToken ", database_file=database_file,
                                  database_table="cloudworks", record_path="integrations", json_path=['meta', 'paging', 'nextUrl'])


# Get Events
AnaplanOps.get_anaplan_paged_data(uri=audit_events_uri, token_type="AnaplanAuthToken ", database_file=database_file,
                                  database_table="events", record_path="response", json_path=['meta', 'paging', 'nextUrl'])


# Exit with return code 0
sys.exit(0)
