"""Quick one-time script to generate token.json for YouTube API access."""
import webbrowser
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]

flow = InstalledAppFlow.from_client_secrets_file("client_secret.json", SCOPES)

print("\n=== YouTube OAuth Authentication ===")
print("A browser window should open. If not, copy the URL printed below.\n")

creds = flow.run_local_server(port=9090, open_browser=True)

with open("token.json", "w") as f:
    f.write(creds.to_json())

print("\ntoken.json created successfully! You can now run icloud_to_youtube_uploader.py")

