import requests

client_id = "78c4jqqf0r4q50"
client_secret = "Jn29kj8cyj9w02aF"
redirect_uri = "https://oauth.pstmn.io/v1/callback"



# Step 1: Request authorization code
auth_params = {
    "response_type": "code",
    "client_id": client_id,
    "redirect_uri": redirect_uri,
    "state": "YOUR_STATE",
    "scope": "r_organization_social r_1st_connections_size r_emailaddress"
}
auth_url = "https://www.linkedin.com/oauth/v2/authorization"
response = requests.get(auth_url, params=auth_params)
authorization_code = input("Enter the authorization code: ")

# Step 2: Exchange authorization code for access token
token_url = "https://www.linkedin.com/oauth/v2/accessToken"
token_params = {
    "grant_type": "authorization_code",
    "code": authorization_code,
    "redirect_uri": redirect_uri,
    "client_id": client_id,
    "client_secret": client_secret
}
response = requests.post(token_url, data=token_params)
access_token = response.json()["access_token"]

print("Access Token:", access_token)