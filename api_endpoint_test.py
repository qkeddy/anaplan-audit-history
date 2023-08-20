import requests
import base64
import json

# Set variables
auth_url = 'https://auth.anaplan.com/token/authenticate'
get_ws_url = 'https://api.anaplan.com/2/0/workspaces'
get_users_url = 'https://api.anaplan.com/scim/1/0/v2/Users?startIndex=1&count=5'
username = 'enter_user_email'
password = 'enter_password'

# Encode credentials
encoded_credentials = str(base64.b64encode((f'{username}:{password}'
                                            ).encode('utf-8')).decode('utf-8'))

# Set headers and payload
headers = { 'Authorization': 'Basic ' + encoded_credentials}
payload = {}

# Login via basic authentication
res = requests.request("POST", url=auth_url, headers=headers, data=payload)

# Isolate access_token
access_token = json.loads(res.text)['tokenInfo']['tokenValue']

print(access_token)

# Set headers for API calls
headers = { 'Authorization': 'Bearer ' + access_token }

# Test Bulk API - Get Workspaces
getWorkspaces = requests.get(url=get_ws_url, headers=headers)
print(getWorkspaces.text)

# Test SCIM API - Get users
getUsers = requests.get(url=get_users_url, headers=headers)
print(getUsers.text)
