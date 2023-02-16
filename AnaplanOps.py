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



# ===  Get Users Function  ===
def get_users(uri):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + AuthToken.Auth.access_token
    }
    res = None

    try: 
        res = requests.get(uri, headers=get_headers)

        data = json.loads(res.text)
        df = pd.json_normalize(data, record_path='Resources')
        columns = df[['id', 'userName', 'displayName']]

        update_table("audit.db3", "users", df=columns)

        logging.info("List of users received")

    except:
        # Check status codes
        StatusExceptions.process_status_exceptions(res, uri)


# ===  Get Audit Events Function  ===
def get_audit_events(uri):
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

        update_table("audit.db3", "events", df=df)

        logging.info("List of events received")

    except:
        # Check status codes
        StatusExceptions.process_status_exceptions(res, uri)



def update_table(database_file, table, df):
    connection = sqlite3.Connection(database_file)
    df.to_sql(name=table, con=connection,if_exists='replace', index=False)
    connection.commit()





