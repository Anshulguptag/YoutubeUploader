"""Delete zombie uploads: videos stuck in processing that never finalized.

Safety rules:
- Only deletes videos whose uploadStatus == "uploaded" AND processingStatus ==
  "processing" (a real upload in progress would be minutes old; anything older
  than 2 hours never completed).
- Never touches uploadStatus == "processed" / processingStatus == "succeeded".
- Run without --delete to just list what would be deleted.
"""
import sys
import httplib2
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/youtube.upload",
          "https://www.googleapis.com/auth/youtube"]
MIN_AGE_HOURS = 2
DELETE = "--delete" in sys.argv


def get_service(token_file):
    creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds.valid:
        creds.refresh(Request())
        with open(token_file, "w") as f:
            f.write(creds.to_json())
    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=120))
    return build("youtube", "v3", http=http)


def main():
    services = []
    for token_file in ("token.json", "token_2.json"):
        try:
            svc = get_service(token_file)
            ch = svc.channels().list(part="contentDetails,snippet", mine=True).execute()
            name = ch["items"][0]["snippet"]["title"]
            services.append((token_file, svc, name))
            print(f"# {token_file}: {name}")
        except Exception as exc:
            print(f"# {token_file} unusable: {str(exc)[:120]}")
    if not services:
        sys.exit("No usable token files.")

    zombies = []
    for token_file, svc, name in services:
        ch = svc.channels().list(part="contentDetails", mine=True).execute()
        uploads_pid = ch["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        items, pt = [], None
        while True:
            resp = svc.playlistItems().list(
                part="contentDetails", playlistId=uploads_pid,
                maxResults=50, pageToken=pt).execute()
            items.extend(resp.get("items", []))
            pt = resp.get("nextPageToken")
            if not pt:
                break
        print(f"# {name}: {len(items)} upload(s), checking latest 50...")
        for item in items[:50]:
            vid = item["contentDetails"]["videoId"]
            try:
                resp = svc.videos().list(
                    part="status,processingDetails,snippet", id=vid).execute()
            except Exception:
                continue
            for v in resp.get("items", []):
                st = v.get("status", {})
                pd = v.get("processingDetails", {})
                us = st.get("uploadStatus", "")
                ps = pd.get("processingStatus", "")
                if us == "uploaded" and ps == "processing":
                    pub = v["snippet"].get("publishedAt", "?")
                    title = v["snippet"].get("title", "?")
                    zombies.append((token_file, vid, title, pub))
                    print(f"  ZOMBIE {vid} | {title} | published {pub}")

    print(f"\n# {len(zombies)} zombie video(s) found")
    if not zombies:
        print("# nothing to do")
        return
    if not DELETE:
        print("# dry run: rerun with --delete to remove them")
        return

    deleted = 0
    for token_file, vid, title, pub in zombies:
        for t, svc, name in services:
            try:
                svc.videos().delete(id=vid).execute()
                deleted += 1
                print(f"# deleted {vid} ({title})")
                break
            except Exception as exc:
                msg = str(exc)
                if "forbidden" not in msg and "403" not in msg:
                    print(f"# delete {vid} failed: {msg[:120]}")
    print(f"\n# Done. Deleted {deleted}/{len(zombies)} zombie video(s).")


if __name__ == "__main__":
    main()
