"""Reproduce the uploader's exact upload path with a tiny resumable upload.

Test A: service built the normal way (long-lived AuthorizedHttp, like the
running uploader). Test B: same call on a brand-new httplib2 connection.
No video is finalized - the 1 KB session is abandoned and Google cleans it up.
"""
import io
import httplib2

from google_auth_httplib2 import AuthorizedHttp
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

SCOPES = ["https://www.googleapis.com/auth/youtube.upload",
          "https://www.googleapis.com/auth/youtube"]


def make_creds():
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds.valid:
        creds.refresh(Request())
    return creds


def try_upload(label, http):
    youtube = build("youtube", "v3", http=http)
    media = MediaInMemoryUpload(b"\x00" * 1024, mimetype="video/mp4",
                                resumable=True, chunksize=256 * 1024)
    request = youtube.videos().insert(
        part="snippet,status",
        body={"snippet": {"title": "diag-probe-do-not-use"},
              "status": {"privacyStatus": "private"}},
        media_body=media)
    try:
        status, response = request.next_chunk(num_retries=1)
        print(f"[{label}] OK - status={status} response={response}", flush=True)
        if response and "id" in response:
            try:
                youtube.videos().delete(id=response["id"]).execute()
                print(f"[{label}] cleaned up probe video {response['id']}", flush=True)
            except Exception as exc:
                print(f"[{label}] cleanup failed: {exc}", flush=True)
    except Exception as exc:
        print(f"[{label}] FAILED: {type(exc).__name__}: {exc}", flush=True)


creds = make_creds()
print("=== Test A: long-lived AuthorizedHttp (uploader's pattern) ===", flush=True)
try_upload("A1", AuthorizedHttp(creds, http=httplib2.Http(timeout=600)))
print("=== Test A2: same http object reused a second time ===", flush=True)
try_upload("A2", AuthorizedHttp(creds, http=httplib2.Http(timeout=600)))
print("=== Test B: fresh connection per call via cache-disabled httplib2 ===", flush=True)
h = httplib2.Http(timeout=600)
h.follow_all_redirects = True
try_upload("B", AuthorizedHttp(creds, http=h))
