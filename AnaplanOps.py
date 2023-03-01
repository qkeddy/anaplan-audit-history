# ===============================================================================
# Description:    Get Anaplan Users
# ===============================================================================

import logging
import requests
import pandas as pd
import json
import sqlite3

import AuthToken
import StatusExceptions


# ===  Load user activity codes from file  ===
def get_usr_activity_codes(database_file):
    df = pd.read_csv('activity_events.csv')
    update_table(database_file=database_file, table="act_codes", df=df, mode='replace')


# ===  Get Users Function  ===
def get_users(uri, database_file):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + AuthToken.Auth.access_token
    }
    res = None

    try: 
        print(uri)
        res = requests.get(uri, headers=get_headers)

        data = json.loads(res.text)
        df = pd.json_normalize(data, record_path='Resources')
        df = df[['id', 'userName', 'displayName']]

        update_table(database_file=database_file, table="users", df=df, mode='replace')

        logging.info("List of users received")

    except:
        # Check status codes
        StatusExceptions.process_status_exceptions(res, uri)


# ===  Get Anaplan Paged Data  ===
def get_anaplan_paged_data(uri, token_type, database_file, database_table, record_path, json_path, workspace_id=None, model_id=None):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': token_type + AuthToken.Auth.access_token
    }
    res = None
    count = 1 

    try:
        # Initial endpoint query
        print(uri)
        res = requests.get(uri, headers=get_headers).json()
        df = pd.json_normalize(res, record_path)
        total_size = res[json_path[0]][json_path[1]]['totalSize']

        while True:
            try:
                # Find key in json path
                next_uri = res[json_path[0]][json_path[1]][json_path[2]]
                
                # Get the next request
                print(next_uri)
                res = requests.get(next_uri, headers=get_headers).json()

                # Normalize data frame
                df_incremental = pd.json_normalize(res, record_path)

                # Append to the existing Data Frame
                df = pd.concat([df, df_incremental], ignore_index=True)
                count +=1

            except KeyError:
                # Stop looping when key cannot be found
                break

            except AttributeError:
                break

            except:
                # Check status codes
                StatusExceptions.process_status_exceptions(res, uri)

        # Drop unsupported SQLite columns from Data Frames
        match database_table:
            case "models": 
                # TODO add logic to drop model tables
                df = df.drop(columns=['categoryValues'])
                update_table(database_file=database_file, table=database_table, df=df, mode='append')
            case "imports" | "exports" | "processes" | "actions":
                df = df[['id', 'name']]
                data = {'workspace_id': workspace_id, 'model_id': model_id }
                df = df.assign(**data)
                update_table(database_file=database_file, table=database_table, df=df, mode='append')
            case "cloudworks":
                # print(df)
                df = df.drop(columns=['schedule.daysOfWeek'])
                # print ('-------------------')
                # print(df)
                update_table(database_file=database_file, table=database_table, df=df, mode='replace')
            case _:
                update_table(database_file=database_file, table=database_table, df=df, mode='replace')
        
        logging.info(f'{total_size} {database_table} records received with {count} API call(s)')
        print(f'{total_size} {database_table} records received with {count} API call(s)')

        # Return IDs for future iterations
        return df['id'].tolist()
        
    except KeyError:
        # Notification when no data is available for a particular API call 
        logging.info(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the {record_path} KeyPath.')
        print(
            f'API call successful, but no {record_path} are available in the Workspace/Model combination. Alternatively, please check the {record_path} KeyPath.')
  
    except:
        # Check status codes
        StatusExceptions.process_status_exceptions(res, uri)


def update_table(database_file, table, df, mode):
    connection = sqlite3.Connection(database_file)
    df.to_sql(name=table, con=connection, if_exists=mode, index=True)
    connection.commit()