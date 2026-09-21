"""One-off diagnostic: check upload/processing status of every video in the playlist."""
import sys
import httplib2
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/youtube.upload",
          "https://www.googleapis.com/auth/youtube"]
PLAYLIST_ID = "PLKwxP3c9cudg"


def get_service(token_file):
    creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds.valid:
        creds.refresh(Request())
        with open(token_file, "w") as f:
            f.write(creds.to_json())
    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=120))
    return build("youtube", "v3", http=http)


def main():
    youtube = None
    for token_file in ("token.json", "token_2.json"):
        try:
            youtube = get_service(token_file)
            youtube.playlistItems().list(part="snippet", playlistId=PLAYLIST_ID,
                                         maxResults=1).execute()
            print(f"# Using {token_file}")
            break
        except Exception as exc:
            print(f"# {token_file} failed: {exc}")
            youtube = None
    if not youtube:
        sys.exit("Could not authenticate with any token file.")

    # Collect playlist videos in order
    items, page_token = [], None
    while True:
        resp = youtube.playlistItems().list(
            part="snippet,contentDetails", playlistId=PLAYLIST_ID,
            maxResults=50, pageToken=page_token).execute()
        items.extend(resp.get("items", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    video_ids = [i["contentDetails"]["videoId"] for i in items]
    print(f"# Playlist has {len(video_ids)} video(s)")

    # Fetch real status per video (mixed ownership: try both tokens per video)
    services = []
    for token_file in ("token.json", "token_2.json"):
        try:
            services.append(get_service(token_file))
            print(f"# {token_file} usable")
        except Exception as exc:
            print(f"# {token_file} failed: {str(exc)[:120]}")
    if not services:
        sys.exit("No usable token files.")

    statuses = {}
    for vid in video_ids:
        for svc in services:
            try:
                resp = svc.videos().list(
                    part="status,processingDetails,snippet", id=vid).execute()
                for v in resp.get("items", []):
                    statuses[v["id"]] = v
                break
            except Exception:
                continue

    stuck = []
    for item in items:
        vid = item["contentDetails"]["videoId"]
        title = item["snippet"].get("title", "?")
        pos = item["snippet"].get("position", "?")
        v = statuses.get(vid)
        if not v:
            print(f"pos {pos:>3} | {title:<22} | {vid} | NOT ACCESSIBLE (other channel/removed?)")
            continue
        st = v.get("status", {})
        pd = v.get("processingDetails", {})
        upload_status = st.get("uploadStatus", "?")
        proc_status = pd.get("processingStatus", "?")
        failure = pd.get("processingFailureReason", "") or ""
        reason = (pd.get("failureReason", "") or st.get("failureReason", "") or "")
        published = v["snippet"].get("publishedAt", "?")
        line = (f"pos {pos:>3} | {title:<22} | {vid} | uploaded {published[:16]} | "
                f"uploadStatus={upload_status} processingStatus={proc_status}")
        if failure:
            line += f" failureReason={failure}"
        if reason:
            line += f" reason={reason}"
        print(line)
        if upload_status in ("processing",) or proc_status in ("processing", "started"):
            if upload_status != "processed":
                stuck.append((title, vid, upload_status, proc_status, failure or reason))
        if upload_status in ("failed", "rejected", "deleted"):
            stuck.append((title, vid, upload_status, proc_status, failure or reason))

    print("\n# ===== RECENT UPLOADS ON EACH CHANNEL (last 30) =====")
    stuck = []
    for idx, svc in enumerate(services, 1):
        try:
            ch = svc.channels().list(part="contentDetails,snippet", mine=True).execute()
            for c in ch.get("items", []):
                uploads_pid = c["contentDetails"]["relatedPlaylists"]["uploads"]
                print(f"# Channel {idx}: {c['snippet'].get('title','?')} uploads={uploads_pid}")
                items2, pt = [], None
                while True:
                    resp = svc.playlistItems().list(
                        part="contentDetails", playlistId=uploads_pid,
                        maxResults=50, pageToken=pt).execute()
                    items2.extend(resp.get("items", []))
                    pt = resp.get("nextPageToken")
                    if not pt:
                        break
                recent_ids = [i["contentDetails"]["videoId"] for i in items2[:30]]
                for vid in recent_ids:
                    try:
                        resp = svc.videos().list(
                            part="status,processingDetails,snippet", id=vid).execute()
                    except Exception:
                        for other in services:
                            if other is svc:
                                continue
                            try:
                                resp = other.videos().list(
                                    part="status,processingDetails,snippet", id=vid).execute()
                                break
                            except Exception:
                                resp = {"items": []}
                    for v in resp.get("items", []):
                        st = v.get("status", {})
                        pd = v.get("processingDetails", {})
                        us = st.get("uploadStatus", "?")
                        ps = pd.get("processingStatus", "?")
                        fr = pd.get("processingFailureReason", "") or st.get("failureReason", "") or ""
                        pub = v["snippet"].get("publishedAt", "?")[:16]
                        title = v["snippet"].get("title", "?")
                        print(f"  {vid} | {title:<22} | {pub} | upload={us} processing={ps} {fr}")
                        if (us in ("failed", "rejected", "deleted")
                                or (us == "uploaded" and ps == "processing")
                                or (us == "processing" and ps != "succeeded")):
                            stuck.append((title, vid, us, ps, fr))
        except Exception as exc:
            print(f"# Channel {idx} check failed: {exc}")

    print("\n# ===== STUCK / FAILED VIDEOS =====")
    if not stuck:
        print("# none found")
    for t in stuck:
        print("#", t)


if __name__ == "__main__":
    main()
