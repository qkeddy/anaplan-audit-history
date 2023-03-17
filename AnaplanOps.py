# ===============================================================================
# Description:    Get Anaplan Users
# ===============================================================================

import logging
import requests
import pandas as pd
import sqlite3

import AuthToken
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
        'Authorization': token_type + AuthToken.Auth.access_token
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
            case "imports" | "exports" | "processes" | "actions":
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
