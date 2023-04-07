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

import Globals
import Utils
import DatabaseOps as db

# Enable logger
logger = logging.getLogger(__name__)


# ===  Fetch audit events from Anaplan. If there are no events then stop process ===
def refresh_events(settings):
    # Set variables
    uris = settings['uris']
    targetModelObjects = settings['targetAnaplanModel']['targetModelObjects']
    database_file = f'{Globals.Paths.databases}/{settings["database"]}'

    # If toggled on, drop events table
    if targetModelObjects['auditData']['tableDrop']:
        db.drop_table(database_file=database_file,
                      table=targetModelObjects['auditData']['table'])

    # Get Events
    latest_run = get_incremental_audit_events(uri=uris['auditEvents'], token_type="AnaplanAuthToken ", database_file=database_file, database_table=targetModelObjects['auditData']['table'],
                                              add_unique_id=targetModelObjects['auditData']['addUniqueId'], mode=targetModelObjects['auditData']['mode'], record_path="response", json_path=['meta', 'paging'], last_run=settings['lastRun'])

    # If there are no events and last_run has not changed, then exit. Otherwise, continue on.
    # latest_run = 0
    if latest_run > settings['lastRun']:

        # Execute the refresh audit data
        refresh_sequence(settings=settings,
                         database_file=database_file,
                         uris=uris,
                         targetModelObjects=targetModelObjects)
        
        execute_process(settings=settings,
                        database_file=database_file)

        # Update `setting.json` with lastRun Date (set by Get Events)
        Utils.update_configuration_settings(
            object=settings, value=latest_run, key='lastRun')


    else:
        print(f'There were no audit events since the last run')
        logging.info(f'There were no audit events since the last run')
    
    
    # TODO Update Anaplan dashboard with the latest run and status


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
    get_anaplan_paged_data(uri=uris['users'], token_type="Bearer ", database_file=database_file,
                                database_table=targetModelObjects['usersData']['table'], add_unique_id=targetModelObjects['usersData']['addUniqueId'], record_path="Resources", page_size_key=['itemsPerPage'], page_index_key=['startIndex'], total_results_key=['totalResults'])

    # Get Workspaces
    workspace_ids = get_anaplan_paged_data(uri=uris['workspaces'], token_type="Bearer ", database_file=database_file,
                                                database_table=targetModelObjects['workspacesData']['table'], add_unique_id=targetModelObjects['workspacesData']['addUniqueId'], record_path="workspaces", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], return_id=True)

    # Get Models in all Workspace
    for ws_id in workspace_ids:
        model_ids = get_anaplan_paged_data(uri=uris['models'].replace('{{workspace_id}}', ws_id), token_type="Bearer ", database_file=database_file,
                                                database_table=targetModelObjects['modelsData']['table'], add_unique_id=targetModelObjects['modelsData']['addUniqueId'], record_path="models", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], return_id=True)

        # Loop through each Model to get details
        for mod_id in model_ids:
                # Get Import Actions in all Models in all Workspaces
                get_anaplan_paged_data(uri=uris['imports'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                            database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="imports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

                # Get Export Actions in all Models in all Workspaces
                get_anaplan_paged_data(uri=uris['exports'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                            database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="exports", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

                # Get Actions in all Models in all Workspaces
                get_anaplan_paged_data(uri=uris['actions'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                        database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="actions", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

                # Get Processes in all Models in all Workspaces
                get_anaplan_paged_data(uri=uris['processes'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                        database_table=targetModelObjects['actionsData']['table'], add_unique_id=targetModelObjects['actionsData']['addUniqueId'], record_path="processes", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

                # Get Files in all Models in all Workspaces
                get_anaplan_paged_data(uri=uris['files'].replace('{{workspace_id}}', ws_id).replace('{{model_id}}', mod_id), token_type="Bearer ", database_file=database_file,
                                        database_table=targetModelObjects['filesData']['table'], add_unique_id=targetModelObjects['filesData']['addUniqueId'], record_path="files", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'], workspace_id=ws_id, model_id=mod_id)

    # Get CloudWorks Integrations
    get_anaplan_paged_data(uri=uris['cloudWorks'], token_type="AnaplanAuthToken ", database_file=database_file,
                                database_table=targetModelObjects['cloudWorksData']['table'], add_unique_id=targetModelObjects['cloudWorksData']['addUniqueId'], record_path="integrations", page_size_key=['meta', 'paging', 'currentPageSize'], page_index_key=['meta', 'paging', 'offset'], total_results_key=['meta', 'paging', 'totalSize'])

    # Fetch ids for target Workspace and Model from the SQLite database
    workspace_id = fetch_ids(
        database_file=database_file, workspace=settings['targetAnaplanModel']['workspace'], type='workspaces')
    model_id = fetch_ids(
        database_file=database_file, model=settings['targetAnaplanModel']['model'], type='models', workspace_id=workspace_id)

    # Fetch Import Data Source ids and loop over each target type and upload data
    write_sample_files = False
    for key in targetModelObjects.values():
        id = fetch_ids(
            database_file=database_file, file=key['importFile'], type='files', workspace_id=workspace_id, model_id=model_id)

        # If a target file is not found in Anaplan, then toggle the creation of sample files
        if id == -1:
                logger.warning(print(
                    "One or more files not found in Anaplan. Creating a sample set of files for upload into Anaplan."))
                print("One or more files not found in Anaplan. Creating a sample set of files for upload into Anaplan.")
                write_sample_files = True
        else:
            if settings["writeSampleFilesOverride"]:
                write_sample_files = True
                logger.info(
                    "Create Sample files is toggled on. Files will be created in the `/samples directory.")
                print("Create Sample files is toggled on. Files will be created in the `/samples directory.")

        # Upload data to Anaplan
        upload_records_to_anaplan(
            database_file=database_file, token_type="Bearer ", write_sample_files=write_sample_files, workspace_id=workspace_id, model_id=model_id, file_id=id, file_name=key['importFile'], table=key['table'], select_all_query=key['selectAllQuery'], add_unique_id=key['addUniqueId'], acronym=key['acronym'], tenant_name=settings['anaplanTenantName'], last_run=settings['lastRun'])


# ===  Load user activity codes from file  ===
def get_usr_activity_codes(database_file, table):
    try:
        df = pd.read_csv(f'{Globals.Paths.scripts}/activity_events.csv')
        db.update_table(database_file=database_file, table=table, df=df, mode='replace')
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# ===  Get Anaplan Audit Events ===
def get_incremental_audit_events(uri, token_type, database_file, database_table, mode, record_path, add_unique_id, json_path, last_run):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': token_type + Globals.Auth.access_token
    }
    res = None
    count = 1
    try:
        # Initial endpoint query
        print(uri)

        # Create empty DataFrame with specific column names & types
        # If additional fields are required, then this will need to be updated.
        df_initialize = pd.DataFrame({'id': pd.Series(dtype='int'), 'eventTypeId': pd.Series(dtype='str'), 'userId': pd.Series(dtype='str'), 'tenantId': pd.Series(dtype='str'), 'objectId': pd.Series(dtype='str'), 'message': pd.Series(dtype='str'), 'success': pd.Series(dtype='bool'), 'errorNumber': pd.Series(dtype='str'), 'ipAddress': pd.Series(dtype='str'), 'userAgent': pd.Series(dtype='str'), 'sessionId': pd.Series(dtype='str'), 'hostName': pd.Series(dtype='str'), 'serviceVersion': pd.Series(dtype='str'), 'eventDate': pd.Series(dtype='int'), 'eventTimeZone': pd.Series(dtype='str'), 'createdDate': pd.Series(dtype='int'), 'createdTimeZone': pd.Series(dtype='str'), 'checksum': pd.Series(dtype='str'), 'objectTypeId': pd.Series(dtype='str'), 'objectTenantId': pd.Series(dtype='str'), 'additionalAttributes.workspaceId': pd.Series(dtype='str'), 'additionalAttributes.actionId': pd.Series(
            dtype='str'), 'additionalAttributes.name': pd.Series(dtype='str'), 'additionalAttributes.type': pd.Series(dtype='str'), 'additionalAttributes.auth_id': pd.Series(dtype='str'), 'additionalAttributes.modelAccessLevel': pd.Series(dtype='str'), 'additionalAttributes.modelId': pd.Series(dtype='str'), 'additionalAttributes.modelRoleName': pd.Series(dtype='str'), 'additionalAttributes.modelRoleId': pd.Series(dtype='str'), 'additionalAttributes.active': pd.Series(dtype='str'), 'additionalAttributes.actionName': pd.Series(dtype='str'), 'additionalAttributes.nux_visible': pd.Series(dtype='str'), 'additionalAttributes.roleId': pd.Series(dtype='str'), 'additionalAttributes.roleName': pd.Series(dtype='str'), 'additionalAttributes.objectTypeId': pd.Series(dtype='str'), 'additionalAttributes.objectTenantId': pd.Series(dtype='str'), 'additionalAttributes.objectId': pd.Series(dtype='str')})



        # Set request with `last_run` value. If last_run is non-zero then increment by 1 millisecond
        if last_run == 0:
            res = requests.post(uri, headers=get_headers, json={"from": last_run})
        else:
            res = requests.post(uri, headers=get_headers, json={"from": last_run+1})

        # Check for unfavorable status codes
        res.raise_for_status()

        # Convert response to JSON and then to a data frame
        res = res.json()
        df = pd.json_normalize(res, record_path)

        # # Append to the initialized Data Frame
        df = pd.concat([df_initialize, df], ignore_index=True)

        total_size = res[json_path[0]][json_path[1]]['totalSize']
        while True:
            try:
                # Find key in json path
                next_uri = res[json_path[0]][json_path[1]]['nextUrl']
                # Get the next request
                print(next_uri)
                res = requests.post(
                    next_uri, headers=get_headers, json={"from": last_run})
                # Check for unfavorable status codes
                res.raise_for_status()
                # Convert response to JSON and then to a data frame
                res = res.json()
                df_incremental = pd.json_normalize(res, record_path)
                # Append to the existing Data Frame
                df = pd.concat([df, df_incremental], ignore_index=True)
                count += 1
            except KeyError:
                # Stop looping when key cannot be found
                break
            except AttributeError:
                break
            except requests.exceptions.HTTPError as err:
                print(
                    f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
                logging.error(
                    f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
                sys.exit(1)
            except requests.exceptions.RequestException as err:
                print(f'{err} in function "{sys._getframe().f_code.co_name}"')
                logging.error(
                    f'{err} in function "{sys._getframe().f_code.co_name}"')
                sys.exit(1)
            except Exception as err:
                print(f'{err} in function "{sys._getframe().f_code.co_name}"')
                logging.error(
                    f'{err} in function "{sys._getframe().f_code.co_name}"')
                sys.exit(1)

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

    except KeyError:
        # Notification when no data is available for a particular API call
        logger.info(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the {record_path} KeyPath.')
        print(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the {record_path} KeyPath.')

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


# ===  Get Anaplan Paged Data  ===
def get_anaplan_paged_data(uri, token_type, database_file, database_table, add_unique_id, record_path, page_size_key, page_index_key, total_results_key, workspace_id=None, model_id=None, return_id=False):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': token_type + Globals.Auth.access_token
    }
    res = None
    count = 1

    try:
        # Initial endpoint query
        logger.info(f'API Endpoint: {uri}')
        print(f'API Endpoint: {uri}')
        res = requests.get(uri, headers=get_headers)

        # Check for unfavorable status codes
        res.raise_for_status()

        # Convert response to JSON and then to a data frame
        res = res.json()
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

        while page_index + page_size < total_results:
            try: 
                # Get next page
                next_uri = f'{uri}&{page_index_key[depth-1].lower()}={page_index + page_size}'
                logger.info(f'API Endpoint: {next_uri}')
                print(f'API Endpoint: {next_uri}')
                res = requests.get(next_uri, headers=get_headers)

                # Check for unfavorable status codes
                res.raise_for_status()

                # Convert response to JSON and then to a data frame
                res = res.json()
                df_incremental = pd.json_normalize(res, record_path)

                # Append to the existing Data Frame
                df = pd.concat([df, df_incremental], ignore_index=True)
                count += 1

                # Get values to retrieve next page
                match depth:
                    case 0: page_index = res[page_index_key[0]]
                    case 3: page_index = res[page_index_key[0]][page_index_key[1]][page_index_key[2]]

            except KeyError | AttributeError as err:
                print(err)
                break
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
            return df['id'].tolist()

    except KeyError:
        # Notification when no data is available for a particular API call
        logger.warning(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the "{record_path}" KeyPath.')
        print(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the "{record_path}" KeyPath.')
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
                        f'"{kwargs["workspace"]}" is an invalid file name and not found in Anaplan.')
                id = row[0]
                print(
                    f'Found Workspace "{kwargs["workspace"]}" with the ID "{id}"')
                logger.info(
                    f'Found Workspace "{kwargs["workspace"]}" with the ID "{id}"')
            case 'models':
                sql = f'SELECT m.id from models m  WHERE m.currentWorkspaceId = "{kwargs["workspace_id"]}" AND m.name = "{kwargs["model"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(
                        f'"{kwargs["model"]}" is an invalid file name and not found in Anaplan.')
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
                        f'"{kwargs["action"]}" is an invalid file name and not found in Anaplan.')
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


# === Query and Load data to Anaplan  ===
def upload_records_to_anaplan(database_file, token_type, write_sample_files, chunk_size=15000, **kwargs):

    # set the SQL query
    if kwargs["select_all_query"]:
        sql = f'SELECT * FROM {kwargs["table"]}'
        rc_sql = f'SELECT count(*) FROM {kwargs["table"]}'
    else:
        # Open SQL File in read mode
        sql_file = open(f'{Globals.Paths.scripts}/audit_query.sql', 'r')
        # read whole file to a string
        sql = sql_file.read()

        # Update sql with tenant name
        sql = sql.replace('{{tenant_name}}',
                          kwargs['tenant_name']).replace('{{time_stamp}}', Globals.Timestamps.gmt_epoch)

        # Update sql with the last run date
        last_run = kwargs['last_run']
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

        # Get the number of chunks and set the Anaplan File Chunk Count
        chunk_count = math.ceil(record_count / chunk_size)

        # Set the file chunk count
        if not write_sample_files:
            body = {'chunkCount': chunk_count}
            uri = f'https://api.anaplan.com/2/0/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{kwargs["file_id"]}'
            res = requests.post(uri, json=body, headers={
                'Content-Type': 'application/json',
                'Authorization': token_type + Globals.Auth.access_token
            })

            # Check for unfavorable status codes
            res.raise_for_status()

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

            uri = f'https://api.anaplan.com/2/0/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{kwargs["file_id"]}/chunks/{count}'
            res = requests.put(uri, data=csv_record_set, headers={
                'Content-Type': 'application/octet-stream',
                'Authorization': token_type + Globals.Auth.access_token
            })

            # Check for unfavorable status codes
            res.raise_for_status()

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


# === Execute Process  ===
def execute_process(settings, database_file):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + Globals.Auth.access_token
    }

    # Fetch Workspace, Model, and Process Ids
    workspace_id = fetch_ids(
        database_file=database_file, workspace=settings['targetAnaplanModel']['workspace'], type='workspaces')
    model_id = fetch_ids(
        database_file=database_file, model=settings['targetAnaplanModel']['model'], type='models', workspace_id=workspace_id)
    process_id = fetch_ids(
        database_file=database_file, action=settings['targetAnaplanModel']['process'], type='actions', workspace_id=workspace_id, model_id=model_id)

    try:
        # Construct URI
        uri = f'{settings["uris"]["integrationApi"]}/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks'

        # Run Process
        res = requests.post(uri, headers=get_headers, json={"localeName": "en_US"})

        # Check for unfavorable status codes
        res.raise_for_status()

        # Isolate task_id
        task_id = json.loads(res.text)['task']['taskId']


        # Monitor Process by looping until complete with a 1 second delay between each loop
        uri = f'{uri}/{task_id}'
        state = 'NOT_STARTED'
        while state != 'COMPLETE':
            # Fetch status
            res = requests.get(uri, headers=get_headers)

            # Check for unfavorable status codes
            res.raise_for_status()

            # Isolate task state
            state = json.loads(res.text)['task']['taskState']

            # Sleep for 1 second
            print('Processing ...')
            time.sleep(1)

        print(f'Audit log refresh is complete')
        logging.info(f'Audit log refresh is complete')

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


# === Upload Time Stamp  ===
def upload_time_stamp(settings, database_file):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + Globals.Auth.access_token
    }

    # Fetch Workspace and Model Ids
    workspace_id = fetch_ids(
        database_file=database_file, workspace=settings['targetAnaplanModel']['workspace'], type='workspaces')
    model_id = fetch_ids(
        database_file=database_file, model=settings['targetAnaplanModel']['model'], type='models', workspace_id=workspace_id)
    
    # Construct base URI
    uri = f'{settings["uris"]["integrationApi"]}/workspaces/{workspace_id}/models/{model_id}/lists'

    # Fetch target list ID
    try:
        # Get Module Lists
        res = requests.get(uri, headers=get_headers)

        # Check for unfavorable status codes
        res.raise_for_status()

        # Get ID of target list 
        list_id = 0
        for key in json.loads(res.text)['lists']:
            if key['name'] == settings['targetAnaplanModel']['batchIdList']:
                list_id = key['id']

    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


    try:
        # Construct URI
        uri = f'{settings["uris"]["integrationApi"]}/workspaces/{workspace_id}/models/{model_id}/lists/{list_id}/items?action=add'

        # Run Process
        res = requests.post(uri, headers=get_headers,
                            json={"items": [{"name": Globals.Timestamps.gmt_epoch, "code": Globals.Timestamps.gmt_epoch}]})

        # Check for unfavorable status codes
        res.raise_for_status()


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
