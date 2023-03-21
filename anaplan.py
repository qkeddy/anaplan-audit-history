# ===============================================================================
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import sys
import logging
import json

import utils
import AnaplanOauth
import Globals
import AnaplanOps

def main():
	# Clear the console
	utils.clear_console()

	# Enable logging
	logger = logging.getLogger(__name__)

	# Get configurations
	settings = utils.read_configuration_settings()
	args = utils.read_cli_arguments()

	device_id_uri = settings['get_device_id_uri']
	tokens_uri = settings['get_tokens_uri']
	register = args.register
	database_file = settings['database']

	uris = settings['uris']
	targetModelObjects = settings['targetAnaplanModel']['targetModelObjects']

	Globals.Auth.client_id = args.client_id
	if args.token_ttl == "":
		Globals.Auth.token_ttl = int(args.token_ttl)

	# If register flag is set, then request the user to authenticate with Anaplan to create device code
	if register:
		logger.info(f'Registering the device with Client ID: {Globals.Auth.client_id}')
		AnaplanOauth.get_device_id(device_id_uri)
		AnaplanOauth.get_tokens(tokens_uri)

	else:
		print('Skipping device registration and refreshing the access_token')
		logger.info('Skipping device registration and refreshing the access_token')
		AnaplanOauth.refresh_tokens(tokens_uri, 0)

	# Start background thread to refresh the `access_token`
	refresh_token = AnaplanOauth.refresh_token_thread(
		1, name="Refresh Token", delay=Globals.Auth.token_ttl, uri=tokens_uri)
	refresh_token.start()

	# Drop tables
	for key in targetModelObjects.values():
		if key['mode']=='append':
			AnaplanOps.drop_table(database_file=database_file, table=key['table'])

	# Load User Activity Codes
	AnaplanOps.get_usr_activity_codes(
		database_file=database_file, table=targetModelObjects['activityCodesData']['table'],
		mode=targetModelObjects['activityCodesData']['mode'])

	# Get Users
	AnaplanOps.get_anaplan_paged_data(uri=uris['users'], token_type="Bearer ", database_file=database_file,
                                   database_table=targetModelObjects['usersData']['table'], record_path="Resources", page_size_key=['itemsPerPage'], page_index_key=['startIndex'], total_results_key=['totalResults'])

	# Get Workspaces
	workspace_ids = AnaplanOps.get_anaplan_paged_data(uri=uris['workspaces'], token_type="Bearer ", database_file=database_file,
                                                   database_table=targetModelObjects['workspacesData']['table'], record_path="workspaces", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], return_id=True)

	# Get Models in all Workspace
	for ws_id in workspace_ids:
		model_ids = AnaplanOps.get_anaplan_paged_data(uri=uris['models'].replace('{{workspace_id}}', ws_id), token_type="Bearer ", database_file=database_file,
                                                database_table=targetModelObjects['modelsData']['table'], record_path="models", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], return_id=True)

		# Loop through each Model to get details
		for mod_id in model_ids:
			# Get Import Actions in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['imports'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['actionsData']['table'], record_path="imports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

			# Get Export Actions in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['exports'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['actionsData']['table'], record_path="exports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

			# Get Actions in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['actions'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['actionsData']['table'], record_path="exports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

			# Get Processes in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['processes'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['actionsData']['table'], record_path="processes", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)
			
			# Get Files in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['files'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['filesData']['table'], record_path="files", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

	# Get CloudWorks Integrations
	AnaplanOps.get_anaplan_paged_data(uri=uris['cloudWorks'], token_type="AnaplanAuthToken ", database_file=database_file,
                                   database_table=targetModelObjects['cloudWorksData']['table'], record_path="integrations", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'])

	# # Get Events
	# AnaplanOps.get_anaplan_paged_data(uri=uris['auditEvents'], token_type="AnaplanAuthToken ", database_file=database_file,
    #                                database_table=targetModelObjects['auditData']['table'], record_path="response", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offSet'], total_results_key=['meta', 'paging', 'totalSize'])


	# TODO Create sample files

	# Fetch ids for target Workspace and Model
	workspace_id = AnaplanOps.fetch_ids(
		database_file=database_file, workspace=settings['targetAnaplanModel']['workspace'], type='workspaces')

	
	model_id = AnaplanOps.fetch_ids(
		database_file=database_file, model=settings['targetAnaplanModel']['model'], type='models', workspace_id=workspace_id)
	
	# TODO add IDs to the other objects
	
	# Fetch Import Action ids
	for key in targetModelObjects.values():
		id = AnaplanOps.fetch_ids(
                    database_file=database_file, action=key['importAction'], type='actions', workspace_id=workspace_id, model_id=model_id)
		
	# Fetch Import Data Source ids
	for key in targetModelObjects.values():
		id = AnaplanOps.fetch_ids(
				database_file=database_file, file=key['importFile'], type='files', workspace_id=workspace_id, model_id=model_id)
		
		if id == '113000000021':
		
			AnaplanOps.upload_records_to_anaplan(
				database_file=database_file, token_type="Bearer ", workspace_id=workspace_id, model_id=model_id, file_id=id)

			
		

	# TODO search for files. If files are not there w/ out IDs, then stop the presses

	
	# 
	# 1) Get Workspaces & Model IDs
	# 2) Get File and Import Action Ids
	# 3) If File Ids and Import Action Ids do not exist then notify the user
	# 5) Else upload file





	

	# Exit with return code 0
	sys.exit(0)


if __name__ == '__main__':
    main()
