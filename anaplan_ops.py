# ===============================================================================
# Description:    Functions to interface with the Anaplan platform and Anaplan content
# ===============================================================================

import logging
import requests
import pandas as pd
import sqlite3
import math
import sys
import json
import time
import re

import globals
import utils
import database_ops as db

# Enable logger
logger = logging.getLogger(__name__)


# ===  Fetch audit events from Anaplan. If there are no events then stop process ===
def refresh_events(settings):
    # Set variables
    uris = settings['uris']
    targetModelObjects = settings['targetAnaplanModel']['targetModelObjects']
    database_file = f'{globals.Paths.databases}/{settings["database"]}'

    # If toggled on, drop events table
    if targetModelObjects['auditData']['tableDrop'] or settings['lastRun']==0:
        db.drop_table(database_file=database_file,
                      table=targetModelObjects['auditData']['table'])

    # Get Events
    latest_run = get_incremental_audit_events(base_uri=uris['auditApi'], database_file=database_file, database_table=targetModelObjects['auditData']['table'],
                                              add_unique_id=targetModelObjects['auditData']['addUniqueId'], mode=targetModelObjects['auditData']['mode'], record_path="response", json_path=['meta', 'paging'], last_run=settings['lastRun'], batch_size=settings['auditBatchSize'])
    logger.info(f'latest_run value: {latest_run}')
    print(f'latest_run value: {latest_run}')

    if latest_run % 1000 != 0:
        logger.error(f'latest run value contains a millisecond')
        print(f'latest run value contains a millisecond')
        logger.error(int(time.time()*1000))
        print(int(time.time()*1000))
    
    # If there are no events and last_run has not changed, then exit. Otherwise, continue on.
    if latest_run > settings['lastRun']:

        # Execute the refresh audit data
        refresh_sequence(settings=settings,
                         database_file=database_file,
                         uris=uris,
                         targetModelObjects=targetModelObjects)
        
        # If `lastRun` is 0, then clear `LOAD_ID` list with the `CT` lists 
        if settings['lastRun']==0:
            execute_process(uri=settings['uris']['integrationApi'],
                            workspace=settings['targetAnaplanModel']['workspace'],
                            model=settings['targetAnaplanModel']['model'],
                            process=settings['targetAnaplanModel']['clearListProcess'],
                            database_file=database_file)
        else:
            execute_process(uri=settings['uris']['integrationApi'],
                            workspace=settings['targetAnaplanModel']['workspace'],
                            model=settings['targetAnaplanModel']['model'],
                            process=settings['targetAnaplanModel']['clearCtListProcess'],
                            database_file=database_file)

        
        # Execute the Process to reload audit data
        execute_process(uri=settings["uris"]["integrationApi"],
                        workspace=settings['targetAnaplanModel']['workspace'],
                        model=settings['targetAnaplanModel']['model'],
                        process=settings['targetAnaplanModel']['process'],
                        database_file=database_file)

        # Upload the latest time stamp to the `Refresh Log`
        print(f'Updating time stamp and record count in Anaplan')
        logging.info(f'Updating time stamp and record count in Anaplan')
        upload_time_stamp(settings=settings, database_file=database_file)

        # Update `setting.json` with lastRun Date (set by Get Events)
        utils.update_configuration_settings(
            object=settings, value=latest_run, key='lastRun')
        
        print(f'Audit log refresh is complete')
        logging.info(f'Audit log refresh is complete')

    else:
        # Nothing changed, so just upload the timestamp
        # Upload the latest time stamp to the `Refresh Log`
        print(f'No new audit logs and only updating the time stamp')
        logging.info(f'No new audit logs and only updating the time stamp')
        upload_time_stamp(settings=settings, database_file=database_file)

        print(f'There were no audit events since the last run')
        logging.info(f'There were no audit events since the last run')
    

# ===  Get Anaplan Audit Events ===
def get_incremental_audit_events(base_uri, database_file, database_table, mode, record_path, add_unique_id, json_path, last_run, batch_size):
    uri = f'{base_uri}/events/search?limit={batch_size}'
    res = None
    count = 1

    try:
        # Set request with `last_run` value. If last_run is non-zero then increment by 1 millisecond
        if last_run > 0:
            last_run = last_run + 1

        # Initial endpoint query
        logger.info(f'uri: {uri}   last run: {last_run}')
        print(f'uri: {uri}   last run: {last_run}')

        # Retrieve first page of audit events
        res = anaplan_api(uri=uri, verb='POST', body={"from": last_run}, token_type="AnaplanAuthToken ").json()

        # Add response to data frame and normalize
        df = pd.json_normalize(res, record_path)

        # Append to the initialized Data Frame
        df = pd.concat([initialize_data_frame(), df], ignore_index=True)

        # Fetch the total number of audit records 
        total_size = res[json_path[0]][json_path[1]]['totalSize']

        # Loop and get audit records until `nextUrl` is not found
        while True:
            try:
                # Find key in json path
                next_uri = res[json_path[0]][json_path[1]]['nextUrl']

                # Get the next request
                print(next_uri)

                # Retrieve the next page of audit events
                res = anaplan_api(uri=next_uri, verb='POST', body={"from": last_run}, token_type="AnaplanAuthToken ").json()

                # Create a temporary data from to hold the incremental records 
                df_incremental = pd.json_normalize(res, record_path)

                # Append to the incremental records to the existing Data Frame
                df = pd.concat([df, df_incremental], ignore_index=True)
                count += 1

            # When `nextUrl` is not found, break the While loop    
            except KeyError:
                # Stop looping when key cannot be found
                break

        # Once all audit records are fetched update the SQLite table
        db.update_table(database_file=database_file, add_unique_id=add_unique_id,
                        table=database_table, df=df, mode=mode)

        logger.info(
            f'{total_size} {database_table} records received with {count} API call(s)')
        print(
            f'{total_size} {database_table} records received with {count} API call(s)')

        # Return last audit event date. If there were no records then simply return the prior last run date.
        if df.shape[0] == 0:
            return last_run
        else:
            return df['eventDate'].tolist()[-1]

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# === Initialize the Data Fram ===
def initialize_data_frame():
    # Create empty DataFrame with specific column names & types
    # If additional fields are required, then this will need to be updated.
    df_initialize = pd.DataFrame({'id': pd.Series(dtype='int'), 'eventTypeId': pd.Series(dtype='str'), 'userId': pd.Series(dtype='str'), 'tenantId': pd.Series(dtype='str'), 'objectId': pd.Series(dtype='str'), 'message': pd.Series(dtype='str'), 'success': pd.Series(dtype='bool'), 'errorNumber': pd.Series(dtype='str'), 'ipAddress': pd.Series(dtype='str'), 'userAgent': pd.Series(dtype='str'), 'sessionId': pd.Series(dtype='str'), 'hostName': pd.Series(dtype='str'), 'serviceVersion': pd.Series(dtype='str'), 'eventDate': pd.Series(dtype='int'), 'eventTimeZone': pd.Series(dtype='str'), 'createdDate': pd.Series(dtype='int'), 'createdTimeZone': pd.Series(dtype='str'), 'checksum': pd.Series(dtype='str'), 'objectTypeId': pd.Series(dtype='str'), 'objectTenantId': pd.Series(dtype='str'), 'additionalAttributes.workspaceId': pd.Series(dtype='str'), 'additionalAttributes.actionId': pd.Series(
        dtype='str'), 'additionalAttributes.name': pd.Series(dtype='str'), 'additionalAttributes.type': pd.Series(dtype='str'), 'additionalAttributes.auth_id': pd.Series(dtype='str'), 'additionalAttributes.modelAccessLevel': pd.Series(dtype='str'), 'additionalAttributes.modelId': pd.Series(dtype='str'), 'additionalAttributes.modelRoleName': pd.Series(dtype='str'), 'additionalAttributes.modelRoleId': pd.Series(dtype='str'), 'additionalAttributes.active': pd.Series(dtype='str'), 'additionalAttributes.actionName': pd.Series(dtype='str'), 'additionalAttributes.nux_visible': pd.Series(dtype='str'), 'additionalAttributes.roleId': pd.Series(dtype='str'), 'additionalAttributes.roleName': pd.Series(dtype='str'), 'additionalAttributes.objectTypeId': pd.Series(dtype='str'), 'additionalAttributes.objectTenantId': pd.Series(dtype='str'), 'additionalAttributes.objectId': pd.Series(dtype='str')})
    
    return df_initialize


# ===  If there are new events then refresh Anaplan object and upload the latest data to Anaplan ===
def refresh_sequence(settings, database_file, uris, targetModelObjects):

    # Drop tables
    for key in targetModelObjects.values():
        if key['tableDrop'] and key['acronym'] != 'AUDIT':
            db.drop_table(database_file=database_file, table=key['table'])

    # Load User Activity Codes
    get_usr_activity_codes(
        database_file=database_file, table=targetModelObjects['activityCodesData']['table'])

    # Get Users
    get_anaplan_paged_data(uri=f'{uris["scimApi"]}/Users', database_file=database_file,
                           database_table=targetModelObjects['usersData']['table'], add_unique_id=targetModelObjects['usersData']['addUniqueId'], record_path="Resources", page_size_key=['itemsPerPage'], page_index_key=['startIndex'], total_results_key=['totalResults'])

    # Get Workspaces
    workspace_ids = get_anaplan_paged_data(uri=f'{uris["integrationApi"]}/workspaces?tenantDetails=true', database_file=database_file,
                                           database_table=targetModelObjects['workspacesData']['table'], add_unique_id=targetModelObjects['workspacesData']['addUniqueId'], record_path="workspaces", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], return_id=True)

    # Get Models in all Workspace
    for ws_id in workspace_ids:
        model_ids = get_anaplan_paged_data(uri=f'{uris["integrationApi"]}/workspaces/{ws_id}/models?modelDetails=true', database_file=database_file,
                                           database_table=targetModelObjects['modelsData']['table'], add_unique_id=targetModelObjects['modelsData']['addUniqueId'], record_path="models", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], return_id=True, workspace_id=1)

        # Loop through each Model to get details
        for mod_id in model_ids:

            # Check the filtering approach
            if settings['workspaceModelFilterApproach'] == "skip":
                # Check if the current WorkspaceId and ModelId are in skip_workspace_model_combos
                if {"WorkspaceId": ws_id, "ModelId": mod_id} in settings['workspaceModelCombos']:
                    print(f"Skipping WorkspaceId: {ws_id}, ModelId: {mod_id} as it is listed to be SKIPPED in the workspace_model_combos")
                    logging.info(f"Skipping WorkspaceId: {ws_id}, ModelId: {mod_id} as it is listed to be SKIPPED in the workspace_model_combos")
                    # Skip this iteration of the loop
                    continue  
            elif settings['workspaceModelFilterApproach'] == "select":
                # Check if the current WorkspaceId and ModelId are in skip_workspace_model_combos
                if {"WorkspaceId": ws_id, "ModelId": mod_id} not in settings['workspaceModelCombos']:
                    if mod_id!=settings['targetAnaplanModel']['model']:
                        print(f"Skipping WorkspaceId: {ws_id}, ModelId: {mod_id} as it is NOT SELECTED in the workspace_model_combos")
                        logging.info(f"Skipping WorkspaceId: {ws_id}, ModelId: {mod_id} as it is NOT SELECTED in the workspace_model_combos")
                        # Skip this iteration of the loop
                        continue  


            # Get Import Actions in all Models in all Workspaces
            get_anaplan_paged_data(uri=f'{uris["integrationApi"]}/workspaces/{ws_id}/models/{mod_id}/imports', database_file=database_file,
                                   database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="imports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

            # Get Export Actions in all Models in all Workspaces
            get_anaplan_paged_data(uri=f'{uris["integrationApi"]}/workspaces/{ws_id}/models/{mod_id}/exports', database_file=database_file,
                                   database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="exports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

            # Get Actions in all Models in all Workspaces
            get_anaplan_paged_data(uri=f'{uris["integrationApi"]}/workspaces/{ws_id}/models/{mod_id}/actions', database_file=database_file,
                                   database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="actions", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

            # Get Processes in all Models in all Workspaces
            get_anaplan_paged_data(uri=f'{uris["integrationApi"]}/workspaces/{ws_id}/models/{mod_id}/processes', database_file=database_file,
                                   database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="processes", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

            # Get Files in all Models in all Workspaces
            get_anaplan_paged_data(uri=f'{uris["integrationApi"]}/workspaces/{ws_id}/models/{mod_id}/files', database_file=database_file,
                                   database_table=targetModelObjects['filesData']['table'], add_unique_id=targetModelObjects['filesData']['addUniqueId'], record_path="files", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

    # Get CloudWorks Integrations
    get_anaplan_paged_data(uri=f'{uris["cloudworksApi"]}/integrations', database_file=database_file,
                           database_table=targetModelObjects['cloudWorksData']['table'], add_unique_id=targetModelObjects['cloudWorksData']['addUniqueId'], record_path="integrations", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'])

    # Fetch ids for target Workspace and Model from the SQLite database
    print(f'Update Anaplan Audit Model')
    logging.info(f'Update Anaplan Audit Model')

    # Set model_id to the target `workspace` value and then check if it is an name or ID
    workspace_id = settings['targetAnaplanModel']['workspace']
    if is_workspace_id(workspace_id):
        workspace_id = fetch_ids(database_file=database_file, workspace=workspace_id, type='workspaces')

    # Set model_id to the target `model` value and then check if it is an name or ID
    model_id = settings['targetAnaplanModel']['model']
    if is_model_id(model_id):
        model_id = fetch_ids(database_file=database_file, model=model_id, type='models', workspace_id=workspace_id)

    # Fetch Import Data Source ids and loop over each target type and upload data
    write_sample_files = False
    for key in targetModelObjects.values():
        id = fetch_ids(
            database_file=database_file, file=key['importFile'], type='files', workspace_id=workspace_id, model_id=model_id)

        # If a target file is not found in Anaplan, then toggle the creation of sample files
        if id == -1:
            logger.warning(print(
                "One or more files not found in Anaplan. Creating a sample set of files for upload into Anaplan."))
            print(
                "One or more files not found in Anaplan. Creating a sample set of files for upload into Anaplan.")
            write_sample_files = True
        else:
            if settings["writeSampleFilesOverride"]:
                write_sample_files = True
                logger.info(
                    "Create Sample files is toggled on. Files will be created in the `/samples directory.")
                print(
                    "Create Sample files is toggled on. Files will be created in the `/samples directory.")

        # Upload data to Anaplan
        upload_records_to_anaplan(base_uri=uris['integrationApi'],
                                  database_file=database_file, write_sample_files=write_sample_files, workspace_id=workspace_id, model_id=model_id, file_id=id, file_name=key['importFile'], table=key['table'], select_all_query=key['selectAllQuery'], add_unique_id=key['addUniqueId'], acronym=key['acronym'], tenant_name=settings['anaplanTenantName'], last_run=settings['lastRun'])


# ===  Check if target model is an ID or a name  ===
def is_model_id(input_str):
    return re.match(r'^[A-Z0-9]{32}$', input_str) is None


# ===  Check if target workspace is an ID or a name  ===
def is_workspace_id(input_str):
    return re.match(r'^[a-z0-9]{32}$', input_str) is None


# ===  Load user activity codes from file  ===
def get_usr_activity_codes(database_file, table):
    try:
        df = pd.read_csv(f'{globals.Paths.scripts}/activity_events.csv')
        db.update_table(database_file=database_file,
                        table=table, df=df, mode='replace')
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# ===  Get Anaplan Paged Data  ===
def get_anaplan_paged_data(uri, database_file, database_table, add_unique_id, record_path, page_size_key, page_index_key, total_results_key, workspace_id=None, model_id=None, return_id=False):
    
    res = None
    count = 1

    try:
        # Initial endpoint query
        logger.info(f'API Endpoint: {uri}')
        print(f'API Endpoint: {uri}')

        # Retrieve first page
        res = anaplan_api(uri=uri, verb="GET", token_type="Bearer ").json()
    
        # Add response to data frame and normalize
        df = pd.json_normalize(res, record_path)

        # Initialize paging variables
        page_size = 0
        page_index = 0
        total_results = 0

        # Set the depth of the key
        depth = len(page_size_key)

        # Match array size for page_size, page_index, total_results
        match depth:
            case 1:
                page_size = res[page_size_key[0]]
                page_index = res[page_index_key[0]] - 1
                total_results = res[total_results_key[0]]
            case 3:
                page_size = res[page_size_key[0]][page_size_key[1]][page_size_key[2]] 
                page_index = res[page_index_key[0]][page_index_key[1]][page_index_key[2]]
                total_results = res[total_results_key[0]][total_results_key[1]][total_results_key[2]]

        # Loop and get records while less than total results of the query
        while page_index + page_size < total_results:
            try: 
                # Get next page
                if '?' in uri:
                    next_uri = f'{uri}&{page_index_key[depth-1]}={page_index + page_size}'
                else: 
                    next_uri = f'{uri}?{page_index_key[depth-1]}={page_index + page_size}'

                logger.info(f'API Endpoint: {next_uri}')
                print(f'API Endpoint: {next_uri}')

                # res = requests.get(next_uri, headers=get_headers)
                res = anaplan_api(uri=next_uri, verb="GET", token_type="Bearer ").json()

                # Create a temporary data from to hold the incremental records
                df_incremental = pd.json_normalize(res, record_path)

                # Append to the incremental records to the existing Data Frame
                df = pd.concat([df, df_incremental], ignore_index=True)
                count += 1

                # Get values to retrieve next page
                match depth:
                    case 1: page_index = res[page_index_key[0]]
                    case 3: page_index = res[page_index_key[0]][page_index_key[1]][page_index_key[2]]

            except KeyError | AttributeError as err:
                print(err)
                break

        # Transform Data Frames columns before updating SQLite
        match database_table:
            case "users":
                df = df[['id', 'userName', 'displayName']]
                db.update_table(database_file=database_file, add_unique_id=add_unique_id,
                             table=database_table, df=df, mode='replace')
            case "models":
                df = df.drop(columns=['categoryValues'])
                db.update_table(database_file=database_file, add_unique_id=add_unique_id,
                             table=database_table, df=df, mode='append')
            case "imports" | "exports" | "processes" | "actions" | "files":
                df = df[['id', 'name']]
                data = {'workspace_id': workspace_id, 'model_id': model_id}
                df = df.assign(**data)
                db.update_table(database_file=database_file,
                             table=database_table, add_unique_id=add_unique_id,df=df, mode='append')
            case "cloudworks":
                df = df.drop(columns=['schedule.daysOfWeek'])
                db.update_table(database_file=database_file, add_unique_id=add_unique_id,
                             table=database_table, df=df, mode='replace')
            case _:
                db.update_table(database_file=database_file, add_unique_id=add_unique_id,
                             table=database_table, df=df, mode='replace')

        logger.info(
            f'{total_results} {database_table} records received with {count} API call(s)')
        print(
            f'{total_results} {database_table} records received with {count} API call(s)')

        # Return Workspace & Model IDs for future iterations
        if return_id:
            if workspace_id==None:
                return df['id'].tolist()
            else:
                filtered_df = df[df['activeState'] != 'ARCHIVED']
                return filtered_df['id'].tolist()
        

    except KeyError:
        # Notification when no data is available for a particular API call
        logger.warning(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the "{record_path}" KeyPath.')
        print(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the "{record_path}" KeyPath.')
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# === Fetch Anaplan object IDs used for uploading data to Anaplan  ===
def fetch_ids(database_file, **kwargs):
    # Establish connection to SQLite
    connection = sqlite3.Connection(database_file)

    # Create a cursor to perform operations on the database
    cursor = connection.cursor()

    # Initialize variables
    sql = ""

    # For each object execute specific SQL to identify the ID
    id = ""
    try:
        match kwargs["type"]:
            case 'workspaces':
                sql = f'SELECT w.id FROM workspaces w where w.name = "{kwargs["workspace"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(
                        f'"{kwargs["workspace"]}" is an invalid Workspace name and not found in Anaplan.')
                id = row[0]
                print(
                    f'Found Workspace "{kwargs["workspace"]}" with the ID "{id}"')
                logger.info(
                    f'Found Workspace "{kwargs["workspace"]}" with the ID "{id}"')
            case 'models':
                sql = f'SELECT m.id from models m WHERE m.currentWorkspaceId = "{kwargs["workspace_id"]}" AND m.name = "{kwargs["model"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(
                        f'"{kwargs["model"]}" is an invalid Model name and not found in Anaplan.')
                id = row[0]
                print(f'Found Model "{kwargs["model"]}" with the ID "{id}"')
                logger.info(
                    f'Found Model "{kwargs["model"]}" with the ID "{id}"')
            case 'actions':
                sql = f'SELECT a.id FROM actions a WHERE a.workspace_id="{kwargs["workspace_id"]}" AND a.model_id="{kwargs["model_id"]}" AND a.name="{kwargs["action"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(
                        f'"{kwargs["action"]}" is an invalid Action name and not found in Anaplan.')
                id = row[0]
                print(
                    f'Found Import Action "{kwargs["action"]}" with the ID "{id}"')
                logger.info(
                    f'Found Import Action "{kwargs["action"]}" with the ID "{id}"')
            case 'files':
                sql = f'SELECT f.id FROM files f WHERE f.workspace_id="{kwargs["workspace_id"]}" AND f.model_id="{kwargs["model_id"]}" AND f.name="{kwargs["file"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(
                        f'"{kwargs["file"]}" is an invalid file name and not found in Anaplan.')
                id = row[0]
                print(f'Found Data File "{kwargs["file"]}" with the ID "{id}"')
                logger.info(
                    f'Found Data File "{kwargs["file"]}" with the ID "{id}"')

        # Close SQLite connection
        connection.close()

        # Return ID
        return id

    except ValueError as ve:
        logger.error(ve)
        print(ve)
        return -1

    except sqlite3.Error as err:
        print(f'SQL error: {err.args} /  SQL Statement: {sql}')
        logger.error(f'SQL error: {err.args} /  SQL Statement: {sql}')

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# === Fetch Anaplan object names of particular IDs  ===
def fetch_names(database_file, **kwargs):
    # Establish connection to SQLite
    connection = sqlite3.Connection(database_file)

    # Create a cursor to perform operations on the database
    cursor = connection.cursor()

    # Initialize variables
    sql = ""

    # For each object execute specific SQL to identify the ID
    name = ""
    try:
        match kwargs["type"]:
            case 'workspaces':
                sql = f'SELECT w.name FROM workspaces w where w.id = "{kwargs["workspace_id"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(
                        f'"{kwargs["workspace_id"]}" is an invalid Workspace ID and not found in Anaplan.')
                name = row[0]

            case 'models':
                sql = f'SELECT m.name from models m WHERE m.id = "{kwargs["model_id"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(
                        f'"{kwargs["model"]}" is an invalid Model ID and not found in Anaplan.')
                name = row[0]

            case 'actions':
                sql = f'SELECT a.name FROM actions a WHERE a.workspace_id="{kwargs["workspace_id"]}" AND a.model_id="{kwargs["model_id"]}" AND a.id="{kwargs["action_id"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(
                        f'"{kwargs["action_id"]}" is an invalid Action ID and not found in Anaplan.')
                name = row[0]

        # Close SQLite connection
        connection.close()

        # Return ID
        return name

    except ValueError as ve:
        logger.error(ve)
        print(ve)
        return -1

    except sqlite3.Error as err:
        print(f'SQL error: {err.args} /  SQL Statement: {sql}')
        logger.error(f'SQL error: {err.args} /  SQL Statement: {sql}')

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# === Query and Load data to Anaplan  ===
def upload_records_to_anaplan(base_uri, database_file, write_sample_files, chunk_size=15000, **kwargs):

    # set the SQL query
    if kwargs["select_all_query"]:
        sql = f'SELECT * FROM {kwargs["table"]}'
        rc_sql = f'SELECT count(*) FROM {kwargs["table"]}'
    else:
        # Open SQL File in read mode
        sql_file = open(f'{globals.Paths.scripts}/audit_query.sql', 'r')
        # read whole file to a string
        sql = sql_file.read()

        # Update sql with tenant name
        sql = sql.replace('{{tenant_name}}',
                          kwargs['tenant_name']).replace('{{time_stamp}}', globals.Timestamps.gmt_epoch)

        # Update sql with the last run date increment by 1 millisecond
        last_run = kwargs['last_run'] + 1
        sql = f'{sql} \nWHERE e.eventDate>{last_run}'
        rc_sql = f'SELECT count(*) FROM events e WHERE e.eventDate>{last_run}'

        # close file
        sql_file.close()

    # Establish connection to SQLite
    connection = sqlite3.Connection(database_file)

    # Create a cursor to perform operations on the database
    cursor = connection.cursor()

    try:
        # Retrieve the record count of events
        cursor.execute(rc_sql)
        record_count = cursor.fetchone()[0]

        # If fetching audit records, then capture record count
        if not kwargs["select_all_query"]:
            globals.Counts.audit_records = record_count

        # Get the number of chunks and set the Anaplan File Chunk Count
        chunk_count = math.ceil(record_count / chunk_size)

        # Set the file chunk count
        if not write_sample_files:
            uri = f'{base_uri}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{kwargs["file_id"]}'
            anaplan_api(uri=uri, verb="POST", body={'chunkCount': chunk_count})

            print(
                f'{record_count} records will be uploaded in {chunk_count} chunks to "{kwargs["file_name"]}"')
            logger.info(
                f'{record_count} records will be uploaded in {chunk_count} chunks to "{kwargs["file_name"]}"')

        # Fetch records and upload to Anaplan by chunk
        count = 0
        while count < chunk_count:
            # Set offset and query chunk
            offset = chunk_size * count
            cursor.execute(f'{sql} \nLIMIT {chunk_size} OFFSET {offset};')

            # Convert query to Pandas Data Frame
            df = pd.DataFrame(cursor.fetchall())
            chunk_row_count = len(df.index)

            # For all object lists, add a unique ID column and start it at 1
            if kwargs["add_unique_id"]:
                df.index = df.index + 1
                df.index.name = f'{kwargs["acronym"]}_CT'

            # If samples files is toggled on, then limit to two records
            if write_sample_files:
                df = df.head(2000)

            # Convert data frame to CSV with no index and if first chunk include the headers
            # Create samples files if option is toggled on
            if count == 0:
                df.columns = [desc[0] for desc in cursor.description]
                if kwargs["add_unique_id"]:
                    if write_sample_files:
                        csv_record_set = df.to_csv(
                            f'./samples/{kwargs["file_name"]}', index=True)
                        break
                    else:
                        csv_record_set = df.to_csv(index=True)
                else:
                    if write_sample_files:
                        csv_record_set = df.to_csv(
                            f'./samples/{kwargs["file_name"]}', index=False)
                        break
                    else:
                        csv_record_set = df.to_csv(index=False)
            else:
                if kwargs["add_unique_id"]:
                    csv_record_set = df.to_csv(index=True, header=False)
                else:
                    csv_record_set = df.to_csv(index=False, header=False)

            # Upload chunk to Anaplan
            uri = f'{base_uri}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{kwargs["file_id"]}/chunks/{count}'
            res = anaplan_api(uri=uri, verb="PUT", data=csv_record_set)

            # If status code 204 is returned, then chunk upload is successful
            if res.status_code == 204:
                print(f'Uploaded: {chunk_row_count} records to "{kwargs["file_name"]}"')
                logger.info(f'Uploaded: {chunk_row_count} records to "{kwargs["file_name"]}"')
            else:
                raise ValueError(f'Failed to upload chunk. Check network connection')

            count += 1

        # Close SQLite connection
        connection.close()

    except ValueError as ve:
        logger.error(ve)
        print(ve)

    except sqlite3.Error as err:
        print(f'SQL error: {err.args} /  SQL Statement: {sql}')
        logger.error(f'SQL error: {err.args} /  SQL Statement: {sql}')
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# === Execute Process  ===
def execute_process(uri, workspace, model, process, database_file):

    # Fetch Workspace, Model, and Process Ids
    if is_workspace_id(workspace):
        workspace_id = fetch_ids(
            database_file=database_file, workspace=workspace, type='workspaces')
    else:
        workspace_id=workspace

    if is_model_id(model):        
        model_id = fetch_ids(
            database_file=database_file, model=model, type='models', workspace_id=workspace_id)
    else:
        model_id = model
    
    process_id = fetch_ids(
        database_file=database_file, action=process, type='actions', workspace_id=workspace_id, model_id=model_id)

    try:
        # Construct URI
        uri = f'{uri}/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks'

        # Run Process
        res = anaplan_api(uri=uri, verb="POST", body={"localeName": "en_US"})

        print(f'Executing process "{process}" belonging to the "{workspace}" / "{model}" combination')
        logger.info(f'Executing process "{process}" belonging to the "{workspace}" / "{model}" combination')

        # Isolate task_id
        task_id = json.loads(res.text)['task']['taskId']

        # Monitor Process by looping until complete with a 1 second delay between each loop
        uri = f'{uri}/{task_id}'
        state = 'NOT_STARTED'
        while state != 'COMPLETE':
            # Fetch status
            res = anaplan_api(uri=uri, verb="GET")

            # Isolate task state
            state = json.loads(res.text)['task']['taskState']

            # Sleep for 1 second
            print('Processing ...')
            time.sleep(1)

        get_process_run_status(uri=uri, database_file=database_file, workspace_id=workspace_id, model_id=model_id)

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# === Get Process results  ===
def get_process_run_status(uri, database_file, workspace_id, model_id):

    try:
        # Get detailed Process status
        res = anaplan_api(uri=uri, verb="GET")

        # Isolate the nested_results
        nested_results = json.loads(res.text)['task']['result']['nestedResults']

        # Loop over each result in nest_results
        for result in nested_results:
            if result['failureDumpAvailable']:
                
                # Fetch the name of the action
                action_name = fetch_names(
                    database_file=database_file, type='actions', workspace_id=workspace_id, model_id=model_id, action_id=result['objectId'])
                print(f'ACTION: {action_name} ({result["objectId"]})')

                # Fetch each error/warning in the nested results
                for sub_result in result['details']:
                    print(f'\t{sub_result["localMessageText"]}')

                    # Look for any missing EVENT_IDs
                    if sub_result['localMessageText'] == 'Item not located in EVENT_ID list':
                        print(f'\t\t{sub_result["values"]}')

                # Write to log file
                logging.warning(
                    f'ACTION: {action_name} ({result["objectId"]}): {result["details"]}')

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# === Upload Time Stamp  ===
def upload_time_stamp(settings, database_file):

    # Fetch Workspace and Model Ids
    if is_workspace_id(settings['targetAnaplanModel']['workspace']):
        workspace_id = fetch_ids(
            database_file=database_file, workspace=settings['targetAnaplanModel']['workspace'], type='workspaces')
    else:
        workspace_id = settings['targetAnaplanModel']['workspace']

    if is_model_id(settings['targetAnaplanModel']['model']):
        model_id = fetch_ids(
            database_file=database_file, model=settings['targetAnaplanModel']['model'], type='models', workspace_id=workspace_id)
    else: 
        model_id = settings['targetAnaplanModel']['model']
    
    # Construct base URI
    base_uri = f'{settings["uris"]["integrationApi"]}/workspaces/{workspace_id}/models/{model_id}'
    
    # Get ID of target Line Items
    uri = f'{base_uri}/lineItems'
    res = anaplan_api(uri, 'GET')
    line_item_id1, line_item_id_2 = 0, 0
    for key in json.loads(res.text)['items']:
        if key['name'] == settings['targetAnaplanModel']['refreshLogLineItems'][0]:
            line_item_id_1 = key['id']
            module_id = key['moduleId']
        if key['name'] == settings['targetAnaplanModel']['refreshLogLineItems'][1]:
            line_item_id_2 = key['id']

    # Get ID of target List
    uri = f'{base_uri}/lists'
    res = anaplan_api(uri, 'GET')
    list_id = 0
    for key in json.loads(res.text)['lists']:
        if key['name'] == settings['targetAnaplanModel']['batchIdList']:
            list_id = key['id']

    # Add the new ID (epoch time) to the `LOAD_ID` list
    uri = f'{base_uri}/lists/{list_id}/items?action=add'
    res = anaplan_api(uri, 'POST', body={"items": [
                      {"name": globals.Timestamps.gmt_epoch, "code": globals.Timestamps.gmt_epoch}]})

    # Inject data to the module
    uri = f'{base_uri}/modules/{module_id}/data'
    res = anaplan_api(uri, 'POST', body=[{"lineItemId": line_item_id_1, "dimensions": [
                      {"dimensionId": list_id, "itemName": globals.Timestamps.gmt_epoch}], "value": globals.Timestamps.local_time_stamp}, {"lineItemId": line_item_id_2, "dimensions": [
                          {"dimensionId": list_id, "itemName": globals.Timestamps.gmt_epoch}], "value": globals.Counts.audit_records}])


# === Interface with Anaplan REST API   ===
def anaplan_api(uri, verb, data=None, body={}, token_type="Bearer "):

    # Set the header based upon the REST API verb    
    if verb == 'PUT':
        get_headers = {
            'Content-Type': 'application/octet-stream',
            'Accept': 'application/json',
            'Authorization': token_type + globals.Auth.access_token
        }
    else: 
        get_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': token_type + globals.Auth.access_token
        }

    # Select operation based upon the the verb
    try:
        match verb:
            case 'GET':
                res = requests.get(uri, headers=get_headers)
            case 'POST':
                res = requests.post(uri, headers=get_headers, json=body)
            case 'PUT':
                res = requests.put(uri, headers=get_headers, data=data)
            case 'DELETE':
                res = requests.delete(uri, headers=get_headers)
            case 'PATCH':
                res = requests.patch(uri, headers=get_headers)
        
        res.raise_for_status()

        return res

    except requests.exceptions.HTTPError as err:
        print(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
        logging.error(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)