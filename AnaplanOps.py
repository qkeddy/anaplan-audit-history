# ===============================================================================
# Description:    Get Anaplan Users
# ===============================================================================

import logging
import requests
import pandas as pd
import sqlite3
import math

import Globals
import StatusExceptions


# ===  Load user activity codes from file  ===
def get_usr_activity_codes(database_file, table, mode):
    df = pd.read_csv('activity_events.csv')
    update_table(database_file=database_file, table="act_codes", df=df, mode='replace')


# ===  Get Anaplan Paged Data  ===
def get_anaplan_paged_data(uri, token_type, database_file, database_table, record_path, page_size_key, page_index_key, total_results_key, workspace_id=None, model_id=None, return_id=False):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': token_type + Globals.Auth.access_token
    }
    res = None
    count = 1

    try:
        # Initial endpoint query
        logging.info(f'API Endpoint: {uri}')
        print(f'API Endpoint: {uri}')
        res = requests.get(uri, headers=get_headers).json()

        # Normalize data frame
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
                logging.info(f'API Endpoint: {next_uri}')
                print(f'API Endpoint: {next_uri}')
                res = requests.get(next_uri, headers=get_headers).json()

                # Normalize data frame
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

            except Exception as err:
                # Check status codes
                print(err)
                StatusExceptions.process_status_exceptions(res, uri)

        # Transform Data Frames columns before updating SQLite
        match database_table:
            case "users":
                df = df[['id', 'userName', 'displayName']]
                update_table(database_file=database_file,
                             table=database_table, df=df, mode='replace')
            case "models":
                df = df.drop(columns=['categoryValues'])
                update_table(database_file=database_file,
                             table=database_table, df=df, mode='append')
            case "imports" | "exports" | "processes" | "actions" | "files":
                df = df[['id', 'name']]
                data = {'workspace_id': workspace_id, 'model_id': model_id}
                df = df.assign(**data)
                update_table(database_file=database_file,
                             table=database_table, df=df, mode='append')
            case "cloudworks":
                df = df.drop(columns=['schedule.daysOfWeek'])
                update_table(database_file=database_file,
                             table=database_table, df=df, mode='replace')
            case _:
                update_table(database_file=database_file,
                             table=database_table, df=df, mode='replace')

        logging.info(
            f'{total_results} {database_table} records received with {count} API call(s)')
        print(
            f'{total_results} {database_table} records received with {count} API call(s)')

        # Return Workspace & Model IDs for future iterations
        if return_id:
            return df['id'].tolist()

    except KeyError:
        # Notification when no data is available for a particular API call
        logging.warning(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the "{record_path}" KeyPath.')
        print(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the "{record_path}" KeyPath.')

    except Exception as err:
        # Check status codes
        print(err)
        StatusExceptions.process_status_exceptions(res, uri)


# ===  Write to tables in the SQLite Database  ===
def update_table(database_file, table, df, mode):
    # Establish connection to SQLite 
    connection = sqlite3.Connection(database_file)

    # Write the contents of Data Frame to the SQLlite table
    df.to_sql(name=table, con=connection, if_exists=mode, index=True)

    # Commit data and close connection
    connection.commit()
    connection.close()


# ===  Drop existing tables in the SQLite Database  ===
def drop_table(database_file, table):
    # Establish connection to SQLite
    connection = sqlite3.Connection(database_file)

    # Create a cursor to perform operations on the database
    cursor = connection.cursor()

    try: 
        # Dropping the specified table
        cursor.execute(f"DROP TABLE {table}")
        logging.info(f'Table `{table}` has been dropped')
        print(f'Table `{table}` has been dropped')
    except:
        logging.warning(f'Table `{table}` does not exist')
        print(f'Table `{table}` does not exist')


# === Query and Load data to Anaplan  ===
def upload_records_to_anaplan(database_file, token_type, chunk_size=15000, **kwargs):
    # Open SQL File in read mode
    sql_file = open("./audit_query.sql", "r")

    # read whole file to a string
    sql = sql_file.read()

    # close file
    sql_file.close()

    # Establish connection to SQLite
    connection = sqlite3.Connection(database_file)

    # Create a cursor to perform operations on the database
    cursor = connection.cursor()

    try: 
        # Retrieve the record count of events
        cursor.execute('SELECT count(*) FROM events')
        record_count = cursor.fetchone()[0]

        # Get the number of chunks and set the Anaplan File Chunk Count
        chunk_count = math.ceil(record_count / chunk_size)
        body = {'chunkCount': chunk_count}
        uri = f'https://api.anaplan.com/2/0/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{kwargs["file_id"]}'
        res = requests.post(uri, json=body, headers={
            'Content-Type': 'application/json',
            'Authorization': token_type + Globals.Auth.access_token
        })
        print(f'{record_count} records will be uploaded in {chunk_count} chunks to {kwargs["file_name"]}')
        logging.info(f'{record_count} records will be uploaded in {chunk_count} chunks to {kwargs["file_name"]}')

        # Fetch records and upload to Anaplan by chunk
        count = 0
        while count < chunk_count:
            # Set offset and query chunk
            offset = chunk_size * count
            cursor.execute(f'{sql} \nLIMIT {chunk_size} OFFSET {offset};')

            # Convert query to Pandas Data Frame
            df = pd.DataFrame(cursor.fetchall())
            chunk_row_count = len(df.index)

            # Convert data frame to CSV with no index and if first chunk include the headers 
            if count == 0:
                df.columns = [desc[0] for desc in cursor.description]
                csv_record_set = df.to_csv(index=False)
            else:
                csv_record_set = df.to_csv(index=False, header=False)

            # Upload to Anaplan
            uri = f'https://api.anaplan.com/2/0/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{kwargs["file_id"]}/chunks/{count}'
            res = requests.put(uri, data=csv_record_set, headers={
                'Content-Type': 'application/octet-stream',
                'Authorization': token_type + Globals.Auth.access_token
            })

            if res.status_code == 204:
                print(f'Uploaded: {chunk_row_count} records to "{kwargs["file_name"]}"')
                logging.info(f'Uploaded: {chunk_row_count} records to "{kwargs["file_name"]}"')
            else:
                raise ValueError(f'Failed to upload chunk. Check network connection')

            count +=1
        
        # Close SQLite connection
        connection.close()
        
    except ValueError as ve:
        logging.error(ve)
        print(ve)

    except sqlite3.Error as err:
        print(f'SQL error: {err.args} /  SQL Statement: {sql}')
        logging.error(f'SQL error: {err.args} /  SQL Statement: {sql}')

    except Exception as err:
        logging.error(f'{err} in function {upload_records_to_anaplan.__name__}')
        print(f'{err} in function {upload_records_to_anaplan.__name__}')


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
                if row is None: raise ValueError(f'"{kwargs["workspace"]}" is an invalid workspace name.')
                id = row[0]
                print(f'Found Workspace "{kwargs["workspace"]}" with the ID "{id}"')
                logging.info(f'Found Workspace "{kwargs["workspace"]}" with the ID "{id}"')
            case 'models':
                sql = f'SELECT m.id from models m  WHERE m.currentWorkspaceId = "{kwargs["workspace_id"]}" AND m.name = "{kwargs["model"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None: raise ValueError(f'"{kwargs["model"]}" is an invalid model name.')
                id = row[0]
                print(f'Found Model "{kwargs["model"]}" with the ID "{id}"')
                logging.info(f'Found Model "{kwargs["model"]}" with the ID "{id}"')
            case 'actions':
                sql = f'SELECT a.id FROM actions a WHERE a.workspace_id="{kwargs["workspace_id"]}" AND a.model_id="{kwargs["model_id"]}" AND a.name="{kwargs["action"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None: raise ValueError(f'"{kwargs["action"]}" is an invalid action name.')
                id = row[0]
                print(f'Found Import Action "{kwargs["action"]}" with the ID "{id}"')
                logging.info(f'Found Import Action "{kwargs["action"]}" with the ID "{id}"')
            case 'files':
                sql = f'SELECT f.id FROM files f WHERE f.workspace_id="{kwargs["workspace_id"]}" AND f.model_id="{kwargs["model_id"]}" AND f.name="{kwargs["file"]}";'
                cursor.execute(sql)
                row = cursor.fetchone()
                if row is None: raise ValueError(f'"{kwargs["file"]}" is an invalid file name.')
                id = row[0]
                print(f'Found Data File "{kwargs["file"]}" with the ID "{id}"')
                logging.info(f'Found Data File "{kwargs["file"]}" with the ID "{id}"')

        # Close SQLite connection
        connection.close()

        # Return ID
        return id


    except ValueError as ve:
        logging.error(ve)
        print(ve)

    except sqlite3.Error as err:
        print(f'SQL error: {err.args} /  SQL Statement: {sql}')
        logging.error(f'SQL error: {err.args} /  SQL Statement: {sql}')

    except Exception as err:
        logging.error(f'{err} in function {fetch_ids.__name__}')
        print(f'{err} in function {fetch_ids.__name__}')

