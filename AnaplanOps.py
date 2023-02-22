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

import sys



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
        columns = df[['id', 'userName', 'displayName']]

        update_table(database_file=database_file, table="users", df=columns)

        logging.info("List of users received")

    except:
        # Check status codes
        StatusExceptions.process_status_exceptions(res, uri)


# ===  Get Audit Events Function  ===
def get_audit_events(uri, database_file):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'AnaplanAuthToken ' + AuthToken.Auth.access_token
    }
    res = None

    try:
        print(uri)
        res = requests.get(uri, headers=get_headers)

        data = json.loads(res.text)
        df = pd.json_normalize(data, record_path='response')

        update_table(database_file=database_file, table="events", df=df)

        logging.info("List of events received")

    except:
        # Check status codes
        StatusExceptions.process_status_exceptions(res, uri)

# =========================================
# ===  POC - Get Audit Events Function  ===
# =========================================
def get_audit_events2(uri, database_file, database_table, record_path):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'AnaplanAuthToken ' + AuthToken.Auth.access_token
    }
    res = None

    try:
        # Initial endpoint query
        print(uri)
        res = requests.get(uri, headers=get_headers).json()
        df = pd.json_normalize(res, record_path)

        while True:
            try:
                next_uri = res['meta']['paging']['nextUrl']
                
                print(next_uri)
                res = requests.get(next_uri, headers=get_headers).json()
                df_incremental = pd.json_normalize(res, record_path)

                # Append to the existing Data Frame
                df = pd.concat([df, df_incremental], ignore_index=True)

            except KeyError:
                break

            except:
                # Check status codes
                StatusExceptions.process_status_exceptions(res, uri)

        update_table(database_file=database_file, table=database_table, df=df)
        logging.info("List of events received")

    except:
        # Check status codes
        StatusExceptions.process_status_exceptions(res, uri)


# ===  Get Workspaces Events Function  ===
def get_workspaces(uri, database_file):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'AnaplanAuthToken ' + AuthToken.Auth.access_token
    }
    res = None

    try:
        print(uri)
        res = requests.get(uri, headers=get_headers)

        data = json.loads(res.text)
        df = pd.json_normalize(data, record_path='workspaces')

        update_table(database_file=database_file, table="workspaces", df=df)

        logging.info("List of events received")

    except:
        # Check status codes
        StatusExceptions.process_status_exceptions(res, uri)


def get_usr_activity_codes(database_file):
    df = pd.read_csv('activity_events.csv')
    print (df)
    update_table(database_file=database_file, table="act_codes", df=df)



def update_table(database_file, table, df):
    connection = sqlite3.Connection(database_file)
    df.to_sql(name=table, con=connection,if_exists='replace', index=False)
    connection.commit()


get_usr_activity_codes("audit.db3")

