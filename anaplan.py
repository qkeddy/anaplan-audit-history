# ===============================================================================
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import sys
import logging

import utils
import AnaplanOauth
import Globals
import AnaplanOps

# TODO add error handling for missing kwargs error
# TODO Test w/ no network connection
# TODO Improve requests error handling
# TODO add execution of a Process
# TODO add logic for just the new records


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
		if key['tableDrop']:   
			AnaplanOps.drop_table(database_file=database_file, table=key['table'])
	
	# Load User Activity Codes
	AnaplanOps.get_usr_activity_codes(
		database_file=database_file, table=targetModelObjects['activityCodesData']['table'])

	# Get Users
	AnaplanOps.get_anaplan_paged_data(uri=uris['users'], token_type="Bearer ", database_file=database_file,
                                   database_table=targetModelObjects['usersData']['table'], add_unique_id=targetModelObjects['usersData']['addUniqueId'], record_path="Resources", page_size_key=['itemsPerPage'], page_index_key=['startIndex'], total_results_key=['totalResults'])

	# Get Workspaces
	workspace_ids = AnaplanOps.get_anaplan_paged_data(uri=uris['workspaces'], token_type="Bearer ", database_file=database_file,
                                                   database_table=targetModelObjects['workspacesData']['table'], add_unique_id=targetModelObjects['workspacesData']['addUniqueId'], record_path="workspaces", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], return_id=True)

	# Get Models in all Workspace
	for ws_id in workspace_ids:
		model_ids = AnaplanOps.get_anaplan_paged_data(uri=uris['models'].replace('{{workspace_id}}', ws_id), token_type="Bearer ", database_file=database_file,
                                                database_table=targetModelObjects['modelsData']['table'], add_unique_id=targetModelObjects['modelsData']['addUniqueId'], record_path="models", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], return_id=True)

		# Loop through each Model to get details
		for mod_id in model_ids:
			# Get Import Actions in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['imports'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="imports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

			# Get Export Actions in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['exports'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="exports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

			# Get Actions in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['actions'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="actions", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

			# Get Processes in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['processes'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="processes", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)
			
			# Get Files in all Models in all Workspaces
			AnaplanOps.get_anaplan_paged_data(uri=uris['files'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                     database_table=targetModelObjects['filesData']['table'], add_unique_id=targetModelObjects['filesData']['addUniqueId'], record_path="files", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

	# Get CloudWorks Integrations
	AnaplanOps.get_anaplan_paged_data(uri=uris['cloudWorks'], token_type="AnaplanAuthToken ", database_file=database_file,
                                   database_table=targetModelObjects['cloudWorksData']['table'], add_unique_id=targetModelObjects['cloudWorksData']['addUniqueId'], record_path="integrations", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'])

		
	# Get Events
	last_run = AnaplanOps.get_incremental_audit_events(uri=uris['auditEvents'], token_type="AnaplanAuthToken ", database_file=database_file,
                                                    database_table=targetModelObjects['auditData']['table'], add_unique_id=targetModelObjects['auditData']['addUniqueId'], mode=targetModelObjects['auditData']['mode'], record_path="response", json_path=['meta', 'paging'], last_run=settings['lastRun'])


	# Fetch ids for target Workspace and Model
	workspace_id = AnaplanOps.fetch_ids(
		database_file=database_file, workspace=settings['targetAnaplanModel']['workspace'], type='workspaces')	
	model_id = AnaplanOps.fetch_ids(
		database_file=database_file, model=settings['targetAnaplanModel']['model'], type='models', workspace_id=workspace_id)
	
	# Fetch Import Data Source ids and loop over each target type and upload data
	write_sample_files = False
	for key in targetModelObjects.values():
		id = AnaplanOps.fetch_ids(
				database_file=database_file, file=key['importFile'], type='files', workspace_id=workspace_id, model_id=model_id)
		
		# If a target file is not found in Anaplan, then toggle the creation of sample files
		if id == -1:
			logger.warning(print("One or more files not found in Anaplan. Creating a sample set of files for upload into Anaplan."))
			print("One or more files not found in Anaplan. Creating a sample set of files for upload into Anaplan.")
			write_sample_files = True
		else:
			if settings["writeSampleFilesOverride"]:
				write_sample_files = True
				logger.info("Create Sample files is toggled on. Files will be created in the `/samples directory.")
				print("Create Sample files is toggled on. Files will be created in the `/samples directory.")

		AnaplanOps.upload_records_to_anaplan(
			database_file=database_file, token_type="Bearer ", write_sample_files=write_sample_files, workspace_id=workspace_id, model_id=model_id, file_id=id, file_name=key['importFile'], table=key['table'], select_all_query=key['selectAllQuery'], add_unique_id=key['addUniqueId'], acronym=key['acronym'], tenant_name=settings['anaplanTenantName'], last_run=settings['lastRun'])
	
	# Update `setting.json` with lastRun Date
	utils.update_configuration_settings(object=settings, value=last_run, key='lastRun')

	# Exit with return code 0
	sys.exit(0)


if __name__ == '__main__':
    main()
