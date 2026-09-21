"""Diagnose the resumable-upload init failure (RedirectMissingLocation).

Sends the exact init request the uploader sends, with httplib2 wire logging,
so we can see the raw status/headers Google returns for the /upload endpoint.
Only creates a resumable session - no data is uploaded, no video is created.
"""
import json
import httplib2

httplib2.debuglevel = 1  # print raw request/response headers

from google_auth_httplib2 import AuthorizedHttp
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/youtube.upload",
          "https://www.googleapis.com/auth/youtube"]

creds = Credentials.from_authorized_user_file("token.json", SCOPES)
if not creds.valid:
    creds.refresh(Request())
http = AuthorizedHttp(creds, http=httplib2.Http(timeout=60))

url = ("https://www.googleapis.com/upload/youtube/v3/videos"
       "?uploadType=resumable&part=snippet,status")
body = json.dumps({
    "snippet": {"title": "diag-probe-do-not-use"},
    "status": {"privacyStatus": "private"},
}).encode()
headers = {
    "Content-Type": "application/json",
    "X-Upload-Content-Type": "video/mp4",
    "X-Upload-Content-Length": "1024",
}

print("\n=== sending resumable init probe ===", flush=True)
try:
    resp, content = http.request(url, "POST", body=body, headers=headers)
    print("\nSTATUS:", resp.status, flush=True)
    print("HEADERS:", dict(resp), flush=True)
    print("BODY:", content[:600], flush=True)
except Exception as exc:
    print(f"\nEXCEPTION: {type(exc).__name__}: {exc}", flush=True)
