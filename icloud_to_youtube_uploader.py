# icloud_to_youtube_uploader.py
"""
Automated uploader:
- Watches the iCloud Drive folder "C:\\Users\\dell\\iCloudDrive\\Ganesh Chaturthi" for new video files.
- Ensures a file is fully downloaded before processing.
- Uploads one video at a time to a YouTube channel playlist.
- After a successful upload, deletes the local copy to save space.
- Video titles follow the pattern: Date_<YYYYMMDD>_<seq>
- Logs all activity to console AND to upload_log.txt
- Tracks uploaded files in uploaded_history.txt to avoid duplicates

Prerequisites:
- Python 3.8+
- pip install watchdog google-auth google-auth-oauthlib google-api-python-client
- A Google Cloud project with YouTube Data API enabled.
- OAuth client credentials file (client_secret.json) placed beside this script.
- The target playlist ID (PLAYLIST_ID) set in the script.
"""

import os
import sys
import time
import logging
import signal
import threading
import queue
import datetime
import random
import re
import socket
import stat
from pathlib import Path
from typing import Optional, Set

import httplib2
from google_auth_httplib2 import AuthorizedHttp

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Google API imports
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# ------------------- Logging Setup -------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, "upload_log.txt")
HISTORY_FILE = os.path.join(SCRIPT_DIR, "uploaded_history.txt")
TITLE_FILE = os.path.join(SCRIPT_DIR, "upload_titles.txt")

logger = logging.getLogger("iCloudUploader")
logger.setLevel(logging.DEBUG)

# Format: timestamp | level | message
formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Console handler (INFO and above)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# File handler (DEBUG and above — captures everything)
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
# -----------------------------------------------------

# ------------------- Configuration -------------------
ICLOUD_FOLDER = r"C:\Users\dell\iCloudDrive\Ganesh Chaturthi"
VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi"}
# OAuth scopes required for uploading and managing playlists
SCOPES = ["https://www.googleapis.com/auth/youtube.upload",
          "https://www.googleapis.com/auth/youtube"]
# Path to OAuth client secret (downloaded from Google Cloud Console)
# Keep each OAuth client paired with its own token.  Absolute paths make this
# work even when the script is launched from another working directory.
CLIENT_SECRETS_FILES = [
    os.path.join(SCRIPT_DIR, "client_secret.json"),
    os.path.join(SCRIPT_DIR, "client_secret_2.json"),
]
TOKEN_FILES = [
    os.path.join(SCRIPT_DIR, "token.json"),
    os.path.join(SCRIPT_DIR, "token_2.json"),
]
# Playlist ID where videos should be added (replace with your own)
PLAYLIST_ID = "PLKwxP3c9cudg"
# Upload chunk size must be a multiple of 256 KB for resumable YouTube uploads.
# Smaller chunks make retries less expensive when a slow connection drops.
CHUNK_SIZE = 256 * 1024 * 8  # 2 MB
UPLOAD_HTTP_TIMEOUT = 10 * 60  # Large iCloud videos can take longer than 60 seconds per request.
MAX_UPLOAD_RETRIES = 8
TITLE_PATTERN = re.compile(r"^Date_(\d{8})_(\d+)$")
# A large sequential read asks iCloud for the entire cloud-only file before
# upload; it avoids random seeks into an unavailable placeholder.
HYDRATION_READ_SIZE = 16 * 1024 * 1024  # 16 MB
HYDRATION_RETRIES = 3
# ----------------------------------------------------

# Global state
video_queue = queue.Queue()
current_account_index = 0
currently_processing = threading.Event()
sequence_number = 1
sequence_date = ""
playlist_titles: Set[str] = set()
# Stats
stats = {"uploaded": 0, "failed": 0, "skipped": 0}
upload_limit_reached = threading.Event()
UPLOAD_LIMIT_COOLDOWN = 24 * 60 * 60  # 24 hours
NEXT_UPLOAD_RETRY_FILE = os.path.join(SCRIPT_DIR, "upload_retry_after.txt")

def request_shutdown(signum, frame):
    """Make Ctrl+C and Ctrl+Break stop the main loop cleanly on Windows."""
    signal_name = getattr(signal, "Signals", lambda value: value)(signum)
    logger.warning(f"Shutdown requested ({signal_name}). Stopping uploader...")
    raise KeyboardInterrupt

def load_history() -> set:
    history = set()
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    # Split by " : " to extract the original filename
                    filename = line.split(" : ")[0].strip()
                    history.add(filename)
    return history

UPLOADED_HISTORY = load_history()

def append_history(filename: str, title: str):
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(f"{filename} : {title}\n")
    UPLOADED_HISTORY.add(filename)

def load_title_map() -> dict:
    titles = {}
    if os.path.exists(TITLE_FILE):
        with open(TITLE_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if " : " in line:
                    filename, title = line.split(" : ", 1)
                    titles[filename] = title
    return titles

TITLE_MAP = load_title_map()

def save_title_map() -> None:
    with open(TITLE_FILE, "w", encoding="utf-8") as f:
        for filename, title in TITLE_MAP.items():
            f.write(f"{filename} : {title}\n")

def get_or_create_title(filename: str) -> str:
    """Return a stable title, avoiding a number already used in the playlist."""
    existing_title = TITLE_MAP.get(filename)
    if existing_title and existing_title not in playlist_titles:
        return existing_title

    title = generate_title()
    TITLE_MAP[filename] = title
    with open(TITLE_FILE, "a", encoding="utf-8") as f:
        f.write(f"{filename} : {title}\n")
    return title

def sync_sequence_from_playlist(youtube) -> None:
    """Read every playlist page and set today's next unused title number."""
    global sequence_number, sequence_date, playlist_titles

    today = datetime.datetime.now().strftime('%Y%m%d')
    highest_sequence = 0
    remote_titles: Set[str] = set()
    page_token = None
    playlist_read_succeeded = False

    try:
        logger.info("Reading playlist titles to continue the upload sequence...")
        while True:
            response = youtube.playlistItems().list(
                part="snippet", playlistId=PLAYLIST_ID, maxResults=50,
                pageToken=page_token,
            ).execute()
            for item in response.get("items", []):
                title = item.get("snippet", {}).get("title", "")
                remote_titles.add(title)
                match = TITLE_PATTERN.fullmatch(title)
                if match and match.group(1) == today:
                    highest_sequence = max(highest_sequence, int(match.group(2)))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        playlist_read_succeeded = True
    except Exception as exc:
        logger.warning(
            f"Could not read playlist sequence ({exc}). Using local title records instead."
        )

    if playlist_read_succeeded:
        # upload_titles.txt was written before each attempt, including failed
        # uploads. Remove those stale reservations so only the playlist decides
        # the next number after a restart.
        stale_filenames = [
            filename for filename in TITLE_MAP if filename not in UPLOADED_HISTORY
        ]
        if stale_filenames:
            for filename in stale_filenames:
                del TITLE_MAP[filename]
            save_title_map()
            logger.info(
                f"Discarded {len(stale_filenames)} title reservation(s) from "
                "unsuccessful earlier uploads."
            )
    else:
        # If the playlist cannot be read, retain local reservations as a safe
        # fallback rather than risk duplicate titles.
        for filename, title in TITLE_MAP.items():
            if filename in UPLOADED_HISTORY or title in remote_titles:
                continue
            match = TITLE_PATTERN.fullmatch(title)
            if match and match.group(1) == today:
                highest_sequence = max(highest_sequence, int(match.group(2)))

    playlist_titles = remote_titles
    sequence_date = today
    sequence_number = highest_sequence + 1
    logger.info(
        f"Playlist sequence ready: {len(remote_titles)} video(s) read; "
        f"next title will be Date_{today}_{sequence_number:03d}."
    )

def correct_playlist_sequence(youtube) -> int:
    """Renumber Date_YYYYMMDD_NNN titles in playlist order, per date.

    Only titles matching the uploader's naming scheme are changed. Video
    descriptions, categories, tags, and language metadata are preserved.
    """
    playlist_items = []
    page_token = None
    while True:
        response = youtube.playlistItems().list(
            part="snippet", playlistId=PLAYLIST_ID, maxResults=50,
            pageToken=page_token,
        ).execute()
        playlist_items.extend(response.get("items", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    planned_changes = []
    date_sequences = {}
    for item in sorted(playlist_items, key=lambda entry: entry["snippet"]["position"]):
        snippet = item["snippet"]
        old_title = snippet.get("title", "")
        match = TITLE_PATTERN.fullmatch(old_title)
        if not match:
            logger.warning(f"Skipping non-uploader title: {old_title!r}")
            continue
        date_key = match.group(1)
        date_sequences[date_key] = date_sequences.get(date_key, 0) + 1
        new_title = f"Date_{date_key}_{date_sequences[date_key]:03d}"
        if new_title != old_title:
            planned_changes.append((snippet["resourceId"]["videoId"], old_title, new_title))

    if not planned_changes:
        logger.info("Playlist titles are already correctly sequenced.")
        return 0

    video_ids = [video_id for video_id, _, _ in planned_changes]
    video_snippets = {}
    for start in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="snippet", id=",".join(video_ids[start:start + 50])
        ).execute()
        video_snippets.update({video["id"]: video["snippet"] for video in response.get("items", [])})

    updated_count = 0
    for video_id, old_title, new_title in planned_changes:
        old_snippet = video_snippets.get(video_id)
        if not old_snippet:
            logger.error(f"Cannot update {old_title}: video details were not returned.")
            continue
        new_snippet = {
            "title": new_title,
            "description": old_snippet.get("description", ""),
            "categoryId": old_snippet["categoryId"],
        }
        for field in ("tags", "defaultLanguage", "defaultAudioLanguage"):
            if field in old_snippet:
                new_snippet[field] = old_snippet[field]
        try:
            youtube.videos().update(
                part="snippet", body={"id": video_id, "snippet": new_snippet}
            ).execute()
            updated_count += 1
            logger.info(f"Renumbered: {old_title} -> {new_title}")
        except HttpError as exc:
            logger.error(
                f"Could not rename {old_title} to {new_title}: {exc}. "
                "Continuing with the remaining playlist videos."
            )

    logger.info(f"Corrected {updated_count} playlist video title(s).")
    return updated_count

def save_retry_after(timestamp: float):
    with open(NEXT_UPLOAD_RETRY_FILE, "w", encoding="utf-8") as f:
        f.write(str(timestamp))

def load_retry_after() -> float:
    try:
        with open(NEXT_UPLOAD_RETRY_FILE, "r", encoding="utf-8") as f:
            return float(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0.0

def clear_retry_after():
    try:
        os.remove(NEXT_UPLOAD_RETRY_FILE)
    except FileNotFoundError:
        pass

def is_upload_limit_error(exc: Exception) -> bool:
    """Return whether YouTube rejected the upload because its quota is exhausted."""
    text = str(exc).lower()
    quota_markers = (
        "uploadlimitexceeded",
        "quotaexceeded",
        "dailylimitexceeded",
        "the user has exceeded the number of videos they may upload",
    )
    return any(marker in text for marker in quota_markers)

def format_size(size_bytes: int) -> str:
    """Convert bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"

def get_authenticated_service() -> Optional[object]:
    """Authenticate and return a YouTube service object."""
    global current_account_index
    if current_account_index >= len(CLIENT_SECRETS_FILES):
        return None
        
    client_secret_file = CLIENT_SECRETS_FILES[current_account_index]
    token_file = TOKEN_FILES[current_account_index]

    if not os.path.exists(client_secret_file):
        logger.error(f"OAuth client credentials file is missing: {client_secret_file}")
        return None
    
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        logger.info(f"Loaded existing credentials from {token_file}")
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info(f"Refreshing expired token for {token_file}...")
            creds.refresh(Request())
            logger.info("Token refreshed successfully")
        else:
            logger.info(f"No valid credentials found for {client_secret_file}. Starting OAuth flow...")
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            creds = flow.run_local_server(port=0)
            logger.info("OAuth authentication completed")
        with open(token_file, 'w') as token:
            token.write(creds.to_json())
        logger.info(f"Credentials saved to {token_file}")
    # The default httplib2 timeout is 60 seconds, which is too short for
    # multi-GB files and slow iCloud/network reads.
    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=UPLOAD_HTTP_TIMEOUT))
    return build('youtube', 'v3', http=http)

def switch_account() -> bool:
    global current_account_index
    if current_account_index + 1 < len(CLIENT_SECRETS_FILES):
        current_account_index += 1
        logger.warning(
            "Quota exhausted for the current client; retrying the same video "
            f"with fallback client: {os.path.basename(CLIENT_SECRETS_FILES[current_account_index])}."
        )
        return True
    return False

def is_video_file(path: Path) -> bool:
    return path.suffix.lower() in VIDEO_EXTENSIONS

def is_icloud_placeholder(file_path: Path) -> bool:
    """Return True when Windows marks an iCloud file as cloud-only/offline."""
    try:
        attributes = getattr(file_path.stat(), "st_file_attributes", 0)
        offline_flag = getattr(stat, "FILE_ATTRIBUTE_OFFLINE", 0x1000)
        return bool(attributes & offline_flag)
    except FileNotFoundError:
        return False

def hydrate_icloud_file(file_path: Path) -> bool:
    """Sequentially read a cloud-only file so iCloud downloads it locally.

    This cannot exceed the user's iCloud/network bandwidth, but it starts a
    full sequential transfer immediately and verifies all bytes are available
    before MediaFileUpload makes resumable seeks through the video.
    """
    if not is_icloud_placeholder(file_path):
        return True

    expected_size = file_path.stat().st_size
    logger.warning(
        f"iCloud cloud-only file detected: {file_path.name}. "
        "Downloading it locally before upload..."
    )
    for attempt in range(1, HYDRATION_RETRIES + 1):
        bytes_read = 0
        last_reported_percent = -1
        try:
            with open(file_path, "rb", buffering=HYDRATION_READ_SIZE) as source:
                while True:
                    block = source.read(HYDRATION_READ_SIZE)
                    if not block:
                        break
                    bytes_read += len(block)
                    percent = int((bytes_read / expected_size) * 100) if expected_size else 100
                    if percent >= last_reported_percent + 5 or percent == 100:
                        logger.info(
                            f"  iCloud download... {percent}% "
                            f"({format_size(bytes_read)}/{format_size(expected_size)})"
                        )
                        last_reported_percent = percent

            if bytes_read == expected_size:
                logger.info(f"iCloud download complete: {file_path.name}")
                return True
            raise OSError(
                f"Read {bytes_read} bytes but expected {expected_size} bytes"
            )
        except (OSError, TimeoutError) as exc:
            if attempt == HYDRATION_RETRIES:
                logger.error(
                    f"Could not fully download {file_path.name} from iCloud: {exc}"
                )
                return False
            delay = attempt * 30
            logger.warning(
                f"iCloud download attempt {attempt}/{HYDRATION_RETRIES} failed "
                f"({exc}); retrying in {delay}s."
            )
            time.sleep(delay)
    return False

def wait_until_fully_downloaded(file_path: Path, timeout: int = 300) -> bool:
    """iCloud placeholders appear as files that grow in size.
    This function checks that the file size stabilizes for a few seconds.
    Returns True if the file appears stable before timeout.
    """
    logger.info(f"Waiting for file to finish downloading: {file_path.name}")
    stable_iterations = 0
    last_size = -1
    start = time.time()
    while time.time() - start < timeout:
        try:
            size = file_path.stat().st_size
        except FileNotFoundError:
            logger.warning(f"File disappeared during download wait: {file_path.name}")
            return False
        if size == last_size:
            stable_iterations += 1
        else:
            stable_iterations = 0
            last_size = size
            logger.debug(f"  File size changing: {format_size(size)}")
        if stable_iterations >= 3:  # size unchanged for ~3 checks (~3 seconds)
            logger.info(f"File is stable at {format_size(size)}: {file_path.name}")
            return hydrate_icloud_file(file_path)
        time.sleep(1)
    logger.warning(f"Timed out ({timeout}s) waiting for file to stabilize: {file_path.name}")
    return False

def generate_title() -> str:
    global sequence_number, sequence_date
    today = datetime.datetime.now().strftime('%Y%m%d')
    if today != sequence_date:
        sequence_date = today
        sequence_number = 1

    # A title generated in this running process is immediately reserved.
    while f"Date_{today}_{sequence_number:03d}" in playlist_titles:
        sequence_number += 1
    title = f"Date_{today}_{sequence_number:03d}"
    playlist_titles.add(title)
    sequence_number += 1
    return title

def is_transient_upload_error(exc: Exception) -> bool:
    """Errors that a resumable upload can safely retry without a new title."""
    if isinstance(exc, HttpError):
        return exc.resp.status in {408, 429, 500, 502, 503, 504}
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, OSError) and exc.errno in {22, 110, 111, 121}:
        return True
    return "timed out" in str(exc).lower()

def upload_video(youtube, file_path: Path, title: str) -> Optional[str]:
    """Uploads a video with progress logging and returns the YouTube videoId on success."""
    file_size = file_path.stat().st_size
    logger.info(f"{'='*50}")
    logger.info(f"UPLOAD STARTED")
    logger.info(f"  File:  {file_path.name}")
    logger.info(f"  Size:  {format_size(file_size)}")
    logger.info(f"  Title: {title}")
    logger.info(f"{'='*50}")

    body = {
        'snippet': {
            'title': title,
            'description': f'Uploaded automatically on {datetime.datetime.now().isoformat()}',
            'tags': ['automated', 'icloud', 'upload'],
            'categoryId': '22'  # People & Blogs
        },
        'status': {
            'privacyStatus': 'private'
        }
    }
    media = MediaFileUpload(str(file_path), chunksize=CHUNK_SIZE, resumable=True)
    upload_start_time = time.time()
    try:
        request = youtube.videos().insert(
            part=','.join(body.keys()), body=body, media_body=media
        )

        response = None
        retry_count = 0
        while response is None:
            try:
                # One immediate retry is handled by the client; the loop below
                # provides visible backoff for longer connection interruptions.
                status, response = request.next_chunk(num_retries=1)
            except Exception as exc:
                if is_upload_limit_error(exc):
                    raise
                if not is_transient_upload_error(exc) or retry_count >= MAX_UPLOAD_RETRIES:
                    raise
                retry_count += 1
                delay = min(300, (2 ** retry_count) + random.uniform(0, 1))
                logger.warning(
                    f"Temporary upload error ({exc}). Keeping the resumable "
                    f"upload and retrying in {int(delay)}s "
                    f"({retry_count}/{MAX_UPLOAD_RETRIES})."
                )
                time.sleep(delay)
                continue
            if status:
                progress = int(status.progress() * 100)
                uploaded_bytes = int(status.progress() * file_size)
                elapsed = time.time() - upload_start_time
                speed = uploaded_bytes / elapsed if elapsed > 0 else 0
                eta = (file_size - uploaded_bytes) / speed if speed > 0 else 0
                logger.info(
                    f"  Uploading... {progress}% "
                    f"({format_size(uploaded_bytes)}/{format_size(file_size)}) "
                    f"| Speed: {format_size(speed)}/s "
                    f"| ETA: {int(eta)}s"
                )

        elapsed_total = time.time() - upload_start_time
        video_id = response.get('id')
        avg_speed = file_size / elapsed_total if elapsed_total > 0 else 0
        logger.info(f"  Uploading... 100% — COMPLETE")
        logger.info(f"UPLOAD SUCCESS")
        logger.info(f"  Video ID:  {video_id}")
        logger.info(f"  URL:       https://youtu.be/{video_id}")
        logger.info(f"  Duration:  {int(elapsed_total)}s")
        logger.info(f"  Avg Speed: {format_size(avg_speed)}/s")
        stats["uploaded"] += 1
        return video_id
    except Exception as e:
        elapsed_total = time.time() - upload_start_time
        logger.error(f"UPLOAD FAILED after {int(elapsed_total)}s")
        logger.error(f"  File:  {file_path.name}")
        logger.error(f"  Error: {e}")
        logger.debug("Upload failure traceback:", exc_info=True)

        if is_upload_limit_error(e):
            logger.error(f"YOUTUBE UPLOAD LIMIT REACHED for account {current_account_index + 1}.")
            return "QUOTA_EXCEEDED"

        stats["failed"] += 1
        return None

def add_to_playlist(youtube, video_id: str):
    body = {
        'snippet': {
            'playlistId': PLAYLIST_ID,
            'resourceId': {
                'kind': 'youtube#video',
                'videoId': video_id
            }
        }
    }
    try:
        youtube.playlistItems().insert(part='snippet', body=body).execute()
        logger.info(f"  Added to playlist: {PLAYLIST_ID}")
    except Exception as e:
        logger.error(f"  Failed to add to playlist: {e}")

def print_stats():
    logger.info(
        f"[STATS] Uploaded: {stats['uploaded']} | "
        f"Failed: {stats['failed']} | "
        f"Skipped: {stats['skipped']} | "
        f"Queue: {video_queue.qsize()} pending"
    )

def process_queue():
    youtube = get_authenticated_service()
    if not youtube:
        logger.error("YouTube authentication failed. Exiting worker thread.")
        return
    logger.info("YouTube API authenticated successfully. Worker thread ready.")
    while True:
        # Do not consume upload attempts while YouTube has imposed an upload limit.
        retry_at = load_retry_after()
        if retry_at > time.time():
            remaining = int(retry_at - time.time())
            logger.warning(
                f"YouTube upload limit is active. Worker sleeping for "
                f"{remaining // 3600}h {(remaining % 3600) // 60}m."
            )
            time.sleep(min(60, max(1, remaining)))
            continue
        elif retry_at:
            clear_retry_after()
            upload_limit_reached.clear()

        file_path: Path = video_queue.get()
        currently_processing.set()
        logger.info(f"--- Processing: {file_path.name} (Queue remaining: {video_queue.qsize()}) ---")
        try:
            while True:
                if file_path.name in UPLOADED_HISTORY:
                    logger.info(f"SKIPPED (already uploaded): {file_path.name}")
                    stats["skipped"] += 1
                    break
                if not wait_until_fully_downloaded(file_path):
                    logger.warning(f"SKIPPED (not stable): {file_path.name}")
                    stats["skipped"] += 1
                    break
                
                title = get_or_create_title(file_path.name)
                video_id = upload_video(youtube, file_path, title)
                
                if video_id == "QUOTA_EXCEEDED":
                    if switch_account():
                        new_youtube = get_authenticated_service()
                        if new_youtube:
                            youtube = new_youtube
                            continue
                        else:
                            logger.error("Failed to authenticate with fallback account.")
                            
                    retry_at = time.time() + UPLOAD_LIMIT_COOLDOWN
                    save_retry_after(retry_at)
                    upload_limit_reached.set()
                    logger.error("=" * 60)
                    logger.error("ALL ACCOUNTS UPLOAD LIMIT REACHED")
                    logger.error("Uploads are paused. Files will NOT be deleted.")
                    logger.error(f"Automatic retry after approx {datetime.datetime.fromtimestamp(retry_at).strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.error("=" * 60)
                    global current_account_index
                    current_account_index = 0
                    video_queue.put(file_path)
                    break
                    
                if video_id:
                    add_to_playlist(youtube, video_id)
                    append_history(file_path.name, title)
                    # Delete to free space
                    try:
                        file_path.unlink()
                        logger.info(f"  Deleted local file: {file_path.name}")
                    except Exception as e:
                        logger.error(f"  Failed to delete {file_path.name}: {e}")
                print_stats()
                break # break the inner while loop to move to next file
        finally:
            currently_processing.clear()
            video_queue.task_done()

class VideoHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            path = Path(event.src_path)
            if is_video_file(path):
                logger.info(f"[DETECTED] New video: {path.name}")
                video_queue.put(path)

    def on_moved(self, event):
        # Handle cases where iCloud finishes download by moving a temp file
        if not event.is_directory:
            path = Path(event.dest_path)
            if is_video_file(path):
                logger.info(f"[DETECTED] Moved video: {path.name}")
                video_queue.put(path)

def scan_existing_videos() -> list:
    """Scan the folder for existing video files and return sorted list."""
    folder = Path(ICLOUD_FOLDER)
    all_files = [f for f in folder.iterdir() if f.is_file()]
    
    videos = []
    skipped_non_video = []
    skipped_already_uploaded = []
    
    for f in all_files:
        if not is_video_file(f):
            skipped_non_video.append(f)
        elif f.name in UPLOADED_HISTORY:
            skipped_already_uploaded.append(f)
        else:
            videos.append(f)
            
    videos.sort(key=lambda f: f.stat().st_mtime)  # oldest first

    logger.info(f"Folder scan: {len(all_files)} total files found")
    logger.info(f"  To upload: {len(videos)} ({', '.join(VIDEO_EXTENSIONS)})")
    logger.info(f"  Skipped (non-video): {len(skipped_non_video)}")
    logger.info(f"  Skipped (already uploaded): {len(skipped_already_uploaded)}")
    if skipped_non_video:
        for f in skipped_non_video:
            logger.debug(f"    Skipped (non-video): {f.name}")
    if skipped_already_uploaded:
        for f in skipped_already_uploaded:
            logger.debug(f"    Skipped (uploaded): {f.name}")
    return videos

def bulk_upload_existing(youtube):
    """Upload all existing videos in the folder before starting the watcher."""
    existing = scan_existing_videos()
    if not existing:
        logger.info("No existing videos found in folder. Skipping bulk upload.")
        return

    logger.info("=" * 50)
    logger.info(f"BULK UPLOAD PHASE — {len(existing)} video(s) to process")
    logger.info("=" * 50)

    for i, file_path in enumerate(existing, 1):
        logger.info(f"\n--- Bulk upload [{i}/{len(existing)}]: {file_path.name} ---")
        try:
            while True:
                if file_path.name in UPLOADED_HISTORY:
                    logger.info(f"SKIPPED (already uploaded): {file_path.name}")
                    stats["skipped"] += 1
                    break
                if not wait_until_fully_downloaded(file_path):
                    logger.warning(f"SKIPPED (not stable): {file_path.name}")
                    stats["skipped"] += 1
                    break
                
                title = get_or_create_title(file_path.name)
                video_id = upload_video(youtube, file_path, title)
                
                if video_id == "QUOTA_EXCEEDED":
                    if switch_account():
                        new_youtube = get_authenticated_service()
                        if new_youtube:
                            youtube = new_youtube
                            continue
                        else:
                            logger.error("Failed to authenticate with fallback account.")
                    
                    retry_at = time.time() + UPLOAD_LIMIT_COOLDOWN
                    save_retry_after(retry_at)
                    upload_limit_reached.set()
                    logger.error("=" * 60)
                    logger.error("ALL ACCOUNTS UPLOAD LIMIT REACHED")
                    logger.error("Uploads are paused. Files will NOT be deleted.")
                    logger.error(f"Automatic retry after approx {datetime.datetime.fromtimestamp(retry_at).strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.error("=" * 60)
                    global current_account_index
                    current_account_index = 0
                    video_queue.put(file_path)
                    break

                if video_id:
                    add_to_playlist(youtube, video_id)
                    append_history(file_path.name, title)
                    try:
                        file_path.unlink()
                        logger.info(f"  Deleted local file: {file_path.name}")
                    except Exception as e:
                        logger.error(f"  Failed to delete {file_path.name}: {e}")
                print_stats()
                break # Exit the while loop for this file
        except Exception as e:
            logger.error(f"  Unexpected error processing {file_path.name}: {e}")
            stats["failed"] += 1

        if upload_limit_reached.is_set():
            break

    logger.info("=" * 50)
    logger.info("BULK UPLOAD PHASE COMPLETE")
    print_stats()
    logger.info("=" * 50)

def main():
    # Ctrl+Break is often delivered more reliably than Ctrl+C while Windows is
    # waiting in a long network/file operation. SIGBREAK exists on Windows only.
    signal.signal(signal.SIGINT, request_shutdown)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, request_shutdown)

    logger.info("=" * 50)
    logger.info("iCloud → YouTube Uploader Started")
    logger.info(f"  Watch folder: {ICLOUD_FOLDER}")
    logger.info(f"  Playlist ID:  {PLAYLIST_ID}")
    logger.info(f"  Log file:     {LOG_FILE}")
    logger.info(f"  History file: {HISTORY_FILE}")
    logger.info(f"  Video types:  {', '.join(VIDEO_EXTENSIONS)}")
    logger.info("=" * 50)

    if not os.path.exists(ICLOUD_FOLDER):
        logger.error(f"Watch folder does not exist: {ICLOUD_FOLDER}")
        logger.error("Please create the folder or update ICLOUD_FOLDER in the script.")
        sys.exit(1)

    # Step 1: Authenticate
    logger.info("Authenticating with YouTube API...")
    youtube = get_authenticated_service()
    if not youtube:
        logger.error("YouTube authentication failed. Exiting.")
        sys.exit(1)
    logger.info("YouTube API authenticated successfully.\n")

    # The playlist is the source of truth for title numbers, including videos
    # uploaded before this script or by a previous script run.
    sync_sequence_from_playlist(youtube)

    # Step 2: Upload all existing videos first
    bulk_upload_existing(youtube)

    # Step 3: Now start watching for new videos
    logger.info("\nSwitching to WATCH MODE — monitoring for new videos...")
    observer = Observer()
    handler = VideoHandler()
    observer.schedule(handler, ICLOUD_FOLDER, recursive=False)
    observer.start()
    logger.info("Folder watcher started. Waiting for new videos...")
    logger.info("Press Ctrl+C to stop.\n")

    # Start worker thread for new files detected by watcher
    threading.Thread(target=process_queue, daemon=True).start()
    try:
        while True:
            # Periodically wake up to check whether a paused upload can resume.
            for _ in range(3600):
                time.sleep(1)
            
            logger.info("\n" + "="*50)
            logger.info("24-HOUR RETRY CYCLE")
            logger.info("Scanning folder for any previously failed videos...")
            logger.info("="*50)
            
            # scan_existing_videos automatically excludes successfully uploaded files
            retry_videos = scan_existing_videos()
            if retry_videos:
                logger.info(f"Queuing {len(retry_videos)} video(s) for retry.")
                for file_path in retry_videos:
                    video_queue.put(file_path)
            else:
                logger.info("No failed videos to retry.")
                
    except KeyboardInterrupt:
        logger.info("\nShutting down...")
        observer.stop()
        print_stats()
        logger.info("Goodbye!")
    observer.join()

if __name__ == "__main__":
    main()
