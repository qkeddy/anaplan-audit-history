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
def get_usr_activity_codes(database_file):
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

            except KeyError:
                    # Stop looping when key cannot be found
                break

            except AttributeError:
                break

            except:
                # Check status codes
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

    except:
        # Check status codes
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
# 1) Query Database and get record count
# 2) Divide 
# 2) Query 15,000 records to create approximately 10MB chunks
# 3) DO While record_index + record_chunk < total_record_count:
# 4) Query with LIMIT
# 5) PUT chunk into Anaplan
# 6) When complete use the `complete` verb
def upload_records_to_anaplan(database_file, chunk_size=15000):
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
        print(f'{record_count} records will be uploaded in {chunk_count} chunks')
        logging.info(f'{record_count} records will be uploaded in {chunk_count} chunks')
        # TODO set Anaplan Chunk Count

        # Fetch records and upload to Anaplan
        count = 0
        while count < chunk_count:
            # Set offset and query chunk
            offset = chunk_size * count
            cursor.execute(f'{sql} \nLIMIT {chunk_size} OFFSET {offset};')
            rows = cursor.fetchall()

            # Upload to Anaplan
            # TODO upload to Anaplan


            print(f'Uploaded: {len(rows)}')
            logging.info(f'Uploaded: {len(rows)}')

            count +=1
        
    except:
        logging.error(f'SQL syntax error')
        print(f'SQL syntax error')


def fetch_ids(database_file, obj_list):
    # Establish connection to SQLite
    connection = sqlite3.Connection(database_file)

    # Create a cursor to perform operations on the database
    cursor = connection.cursor()

    # Initialize variables
    sql = ""

    # For each object execute specific SQL to identify the ID
    try:
        for obj in obj_list:
            match obj[1]:
                case 'workspaces':
                    sql = f'SELECT w.id FROM workspaces w where w.name = "{obj[0]}";'
                    cursor.execute(sql)
                    row = cursor.fetchone()
                    if row is None: raise ValueError(f'"{obj[0]}" is an invalid {obj[1]} name.')
                    Globals.Ids.workspace_id = row[0]
                    print(f'Found Workspace "{obj[0]}" with the ID "{Globals.Ids.workspace_id}"')
                    logging.info(f'Found Workspace "{obj[0]}" with the ID "{Globals.Ids.workspace_id}"')
                case 'models':
                    sql = f'SELECT m.id from models m  WHERE m.currentWorkspaceId = "{Globals.Ids.workspace_id}" AND m.name = "{obj[0]}";'
                    cursor.execute(sql)
                    row = cursor.fetchone()
                    if row is None: raise ValueError(f'"{obj[0]}" is an invalid {obj[1]} name.')
                    Globals.Ids.model_id = row[0]
                    print(f'Found Model "{obj[0]}" with the ID "{Globals.Ids.model_id}"')
                    logging.info(f'Found Model "{obj[0]}" with the ID "{Globals.Ids.model_id}"')
                case 'actions':
                    sql = f'SELECT a.id FROM actions a WHERE a.workspace_id="{Globals.Ids.workspace_id}" AND a.model_id="{Globals.Ids.model_id}" AND a.name="{obj[0]}";'
                    cursor.execute(sql)
                    row = cursor.fetchone()
                    if row is None: raise ValueError(f'"{obj[0]}" is an invalid {obj[1]} name.    {sql}')
                    cursor.execute(sql)
                    Globals.Ids.import_action_id = row[0]
                    print( f'Found Import Action "{obj[0]}" with the ID "{Globals.Ids.import_action_id}"')
                    logging.info(f'Found Import Action "{obj[0]}" with the ID "{Globals.Ids.import_action_id}"')
                case 'files':
                    sql = f'SELECT f.id FROM files f WHERE f.workspace_id="{Globals.Ids.workspace_id}" AND f.model_id="{Globals.Ids.model_id}" AND f.name="{obj[0]}";'
                    cursor.execute(sql)
                    row = cursor.fetchone()
                    if row is None: raise ValueError(f'"{obj[0]}" is an invalid {obj[1]} name.  {sql}')
                    Globals.Ids.file_id = row[0]
                    print(f'Found Data File "{obj[0]}" with the ID "{Globals.Ids.file_id}"')
                    logging.info(f'Found Data File "{obj[0]}" with the ID "{Globals.Ids.file_id}"')

    except ValueError as ve:
        logging.error(ve)
        print(ve)

    except sqlite3.Error as err:
        print(f'SQL error: {err.args} /  SQL Statement: {sql}')
        logging.error(f'SQL error: {err.args} /  SQL Statement: {sql}')

    except Exception as err:
        logging.error(f'{err} in function {fetch_ids.__name__}')
        print(f'{err} in function {fetch_ids.__name__}')

