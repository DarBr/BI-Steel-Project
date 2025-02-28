
# import necessary packages
import requests
import sys
import os


# add your Client-ID and Client-secret from the API Client configuration GUI to
# your environment variable first
IPNT_CLIENT_ID = os.environ.get('IPNT_CLIENT_ID')
IPNT_CLIENT_SECRET = os.environ.get('IPNT_CLIENT_SECRET')

ACCESS_TOKEN_URL = "https://identity.netztransparenz.de/users/connect/token"


# Ask for the token providing above authorization data
response = requests.post(ACCESS_TOKEN_URL,
                data = {
                        'grant_type': 'client_credentials',
                        'client_id': IPNT_CLIENT_ID,
                        'client_secret': IPNT_CLIENT_SECRET
        })

# Parse the token from the response if the response was OK 
if response.ok:
    TOKEN = response.json()['access_token']
else:
    print(f'Error retrieving token\n{response.status_code}:{response.reason}',
        file = sys.stderr)
    exit(-1)

# Provide URL to request health info on API
myURL = "https://ds.netztransparenz.de/api/v1/health"
response = requests.get(myURL, headers = {'Authorization': 'Bearer {}'.format(TOKEN)})
print(response.text, file = sys.stdout)
