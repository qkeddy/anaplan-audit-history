import requests
import base64


# Set variables
url = "https://auth.anaplan.com/token/authenticate"
username = 'quin.eddy@anaplan.com'
password = 'Q00l83@ns$3!'

# Encode credentials
encoded_credentials = str(base64.b64encode((f'{username}:{password}'
                                            ).encode('utf-8')).decode('utf-8'))

# Set headers and payload
headers = {
    'Authorization': 'Basic ' + encoded_credentials
}
payload = {}


# Login via basic authentication
response = requests.request("POST", url, headers=headers, data=payload)
# response = requests.post(url, headers=headers, data=payload)
print(response.text)
