# icloud_to_youtube_uploader.py
"""
Automated uploader:
- Watches the iCloud Drive folder "C:\\Users\\dell\\iCloudDrive\\Ganesh Chaturthi" for new video files.

- Processes strictly ONE video at a time: the video is downloaded from iCloud,
  uploaded, and offloaded before the next video is touched.
- After a successful upload, the local disk space is freed by dehydrating the
  video back to iCloud (the same as Explorer's "Free up space"). The video is
  NOT deleted from iCloud; it is downloaded again only if it is needed.
- If a video fails (download or upload), its local copy is OFFLOADED (not deleted):
  the same "Free up space" unpin is applied so the video STAYS in iCloud, the
  failure is recorded in failed_videos.txt, and the next video is started. Videos
  that failed earlier are skipped, so they are never downloaded twice.

  IMPORTANT: a video file is NEVER deleted anywhere in this script. Deleting a
  file inside the iCloud Drive folder would also delete it from iCloud. The
  only cleanup that ever happens is offloading (unpinning) the local copy, which
  keeps the video safely in the cloud.

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
import ctypes
import subprocess
import shutil
import stat

from ctypes import wintypes
from pathlib import Path

from typing import Optional, Set

import httplib2
from google_auth_httplib2 import AuthorizedHttp

# Patch httplib2 to handle 3xx redirects that lack a Location header.
# YouTube's API sometimes returns redirect responses (307/308) without a
# Location: header. By default httplib2 raises RedirectMissingLocation, which
# would abort the entire resumable upload session. Instead, we convert this
# to a ServerNotFoundError, which httplib2/googleapiclient treat as a
# transient error and retry automatically.
_orig_request = httplib2.Http.request
def _patched_request(self, uri, method='GET', body=None, headers=None,
                     **_kwargs):
    try:
        return _orig_request(self, uri, method=method, body=body,
                             headers=headers, **_kwargs)
    except httplib2.error.RedirectMissingLocation:
        raise httplib2.ServerNotFoundError(
            'YouTube returned a redirect without a Location header; '
            'treating as a transient network error for retry.'
        )
httplib2.Http._orig_request = _orig_request
httplib2.Http.request = _patched_request



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
FAILED_FILE = os.path.join(SCRIPT_DIR, "failed_videos.txt")
# Plain local folder (OUTSIDE iCloud) that holds a temporary copy of exactly
# one video while it is uploaded. Copying the file out of the iCloud folder
# first prevents mid-upload read failures (OSError [Errno 22]) that iCloud's
# placeholder handling can cause; such failures leave broken "stuck
# processing" videos on YouTube. The copy is deleted again right after the
# upload, so only one staged file ever occupies disk space.
STAGING_DIR = os.path.join(SCRIPT_DIR, "upload_staging")

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
# A hydration read that produces no bytes for this long is considered wedged
# (iCloud's placeholder driver can block forever). The attempt is abandoned so
# the rest of the queue keeps flowing; the file can be retried on a later run.
HYDRATION_STALL_TIMEOUT = 10 * 60

# Videos are processed strictly one at a time: download from iCloud, upload,
# then clean up. These settings control that single-video pipeline.
DOWNLOAD_WAIT_TIMEOUT = 5 * 60  # Seconds to wait for iCloud to finish a file.
MAX_DOWNLOAD_ATTEMPTS = 3       # Download tries before a video counts as failed.
DOWNLOAD_RETRY_DELAY = 30       # Seconds between download attempts.

# Free the local disk space of every processed video by dehydrating it back to
# iCloud, exactly like Explorer's "Free up space". The video itself is NOT
# removed from iCloud: only the local download is dropped, and opening or
# re-uploading the video downloads it again.
# NOTE: video files are never deleted by this script. Deleting a file inside
# the iCloud Drive folder would delete it from iCloud as well; only the
# offload (unpin) above is ever used to free space, including for failures.
OFFLOAD_LOCAL_COPY = True
OFFLOAD_WAIT_TIMEOUT = 60       # Seconds to wait for iCloud to free the space.
OFFLOAD_POLL_INTERVAL = 2       # Seconds between progress checks.
# Videos that failed to download or upload are listed in failed_videos.txt and
# skipped by later scans so the same file is not downloaded and attempted again.
# Delete an entry (or the whole file) to let the uploader retry that video.
SKIP_FAILED_VIDEOS = True
# attrib.exe is the command-line equivalent of Explorer's "Free up space".
# Unpinning an iCloud file (+U -P) makes iCloud drop the local download.
ATTRIB_EXE = os.path.join(
    os.environ.get("SystemRoot", r"C:\Windows"), "System32", "attrib.exe"
)
# Windows Files On-Demand attributes that a cloud provider (iCloud Drive) sets.
FILE_ATTRIBUTE_PINNED = 0x00080000
FILE_ATTRIBUTE_UNPINNED = 0x00100000
FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x00400000
CLOUD_FILE_ATTRIBUTES = (
    FILE_ATTRIBUTE_PINNED | FILE_ATTRIBUTE_UNPINNED | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
)
# How many parent folders to check when looking for those cloud attributes.
CLOUD_FOLDER_LOOKUP_DEPTH = 4
# GetCompressedFileSizeW reports the bytes a file actually occupies on this
# disk; iCloud Drive keeps reporting the full logical size while the data is
# still in the cloud, so this is how downloaded files are told apart.
if os.name == "nt":
    _KERNEL32 = ctypes.WinDLL("kernel32.dll", use_last_error=True)
    _KERNEL32.GetCompressedFileSizeW.restype = wintypes.DWORD
    _KERNEL32.GetCompressedFileSizeW.argtypes = [
        wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)
    ]
else:
    _KERNEL32 = None

# ----------------------------------------------------

# Global state
video_queue = queue.Queue()
# Names already waiting in the queue, so the same video is never processed twice.
queued_video_names: Set[str] = set()
queued_lock = threading.Lock()
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

def load_failed_videos() -> set:
    """Return the names of videos that failed earlier and should be skipped."""
    failed = set()
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                entry = line.strip()
                if entry:
                    failed.add(entry.split(" : ")[0].strip())
    return failed

FAILED_VIDEOS = load_failed_videos() if SKIP_FAILED_VIDEOS else set()

def append_history(filename: str, title: str):
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(f"{filename} : {title}\n")
    UPLOADED_HISTORY.add(filename)

def record_failure(filename: str, reason: str) -> None:
    """Keep a record of videos that failed, so they are not attempted forever."""
    FAILED_VIDEOS.add(filename)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(FAILED_FILE, "a", encoding="utf-8") as f:
            f.write(f"{filename} : {reason} : {timestamp}\n")
    except OSError as exc:
        logger.error(f"Could not write {FAILED_FILE}: {exc}")

def local_disk_usage(file_path: Path) -> Optional[int]:
    """Return how many bytes of a file are really stored on this disk.

    iCloud Drive can report the full logical size while the data is still in the
    cloud, so the allocated size is what shows whether a video is downloaded,
    only partly downloaded, or free of local data.
    """
    if _KERNEL32 is None:
        return None
    try:
        high = wintypes.DWORD(0)
        low = _KERNEL32.GetCompressedFileSizeW(str(file_path), ctypes.byref(high))
    except (OSError, ValueError):
        return None
    if low == 0xFFFFFFFF and ctypes.get_last_error() != 0:
        return None
    return (high.value << 32) | low

def is_cloud_managed_file(file_path: Path) -> bool:
    """Return True when a cloud provider (iCloud Drive) tracks this file.

    Windows marks Files On-Demand files with cloud attributes, but a file that
    iCloud has not converted yet - one that has just been created locally, for
    example - only inherits them from the iCloud folder around it, so the parent
    folders are checked as well.
    """
    candidate = file_path
    for _ in range(CLOUD_FOLDER_LOOKUP_DEPTH):
        try:
            attributes = getattr(candidate.stat(), "st_file_attributes", 0)
        except FileNotFoundError:
            return False
        if attributes & CLOUD_FILE_ATTRIBUTES:
            return True
        if candidate.parent == candidate:
            break
        candidate = candidate.parent
    return False

def offload_local_file(file_path: Path, reason: str) -> bool:
    """Free the local disk space of a video WITHOUT removing it from iCloud.

    Unpinning the file (`attrib +U -P`) is exactly what Explorer's "Free up
    space" does: iCloud drops the local download while the video stays in iCloud
    and is downloaded again later if it is needed.

    SAFETY: this function never deletes the file. Deleting a file inside the
    iCloud Drive folder would also delete it from iCloud. If the offload cannot
    be performed, the local copy is simply left untouched and the failure is
    logged; the caller moves on to the next video either way.
    """
    if not file_path.exists():
        return True
    if not OFFLOAD_LOCAL_COPY:
        logger.info(
            f"  Keeping the local copy of {file_path.name} (OFFLOAD_LOCAL_COPY=False)."
        )
        return False

    usage = local_disk_usage(file_path)
    if usage == 0:
        logger.info(f"  No local data to free ({reason}): {file_path.name}")
        return True
    if usage is None:
        logger.warning(
            f"  Cannot read the local size of {file_path.name}; leaving it untouched."
        )
        return False
    if not is_cloud_managed_file(file_path):
        logger.warning(
            f"  {file_path.name} is not managed by iCloud Drive, so its local copy "
            "cannot be offloaded automatically."
        )
        return False

    logger.info(
        f"  Offloading {format_size(usage)} back to iCloud ({reason}): {file_path.name}"
    )
    try:
        result = subprocess.run(
            [ATTRIB_EXE, "+U", "-P", str(file_path)],
            capture_output=True, text=True, timeout=OFFLOAD_WAIT_TIMEOUT,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.error(f"  Could not unpin {file_path.name}: {exc}")
        return False
    if result.returncode != 0:
        logger.warning(
            f"  attrib returned {result.returncode} for {file_path.name}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    deadline = time.time() + OFFLOAD_WAIT_TIMEOUT
    while time.time() < deadline:
        remaining = local_disk_usage(file_path)
        if remaining == 0:
            if not file_path.exists():
                # Should never happen: the offload must keep the iCloud
                # placeholder on disk. Raise a loud alarm if the file vanished.
                logger.critical(
                    f"  SAFETY ALERT: {file_path.name} disappeared from the iCloud "
                    "folder during offload! It should still exist as a cloud-only "
                    "placeholder. Check icloud.com immediately and do not treat "
                    "this upload as fully verified."
                )
                return False
            logger.info(
                f"  Freed {format_size(usage)} locally ({reason}). "
                f"{file_path.name} stays in iCloud."
            )
            return True
        time.sleep(OFFLOAD_POLL_INTERVAL)

    logger.warning(
        f"  iCloud has not freed the local copy of {file_path.name} yet ({reason}). "
        "It should happen in the background; you can also right-click the file in "
        'Explorer and choose "Free up space".'
    )
    return False

def drop_failed_video(file_path: Path, reason: str) -> None:
    """Record a failed video, offload its local copy, and carry on with the rest.

    The video STAYS in iCloud. Its name goes into failed_videos.txt so later
    scans skip it instead of downloading the same file again, and the local copy
    is offloaded (unpinned, not deleted) so a failure never keeps disk space
    busy. The next video is then processed immediately.
    """
    record_failure(file_path.name, reason)
    offload_local_file(file_path, reason)

def enqueue_video(file_path: Path, reason: str = "") -> bool:
    """Queue one video, ignoring a video that is already waiting to be handled."""
    with queued_lock:
        if file_path.name in queued_video_names:
            logger.debug(f"Already queued, ignoring duplicate: {file_path.name}")
            return False
        queued_video_names.add(file_path.name)
    suffix = f" ({reason})" if reason else ""
    logger.info(f"[QUEUED] {file_path.name}{suffix}")
    video_queue.put(file_path)
    return True

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
    """Return True when this file is not fully stored on the local disk yet.

    iCloud Drive reports the full logical size even while the video is still in
    the cloud, so a file only counts as downloaded when the bytes on disk match
    the logical size.
    """
    try:
        logical_size = file_path.stat().st_size
    except FileNotFoundError:
        return False
    usage = local_disk_usage(file_path)
    if usage is not None:
        return usage < logical_size
    # Fall back to the provider's own offline flag when the allocated size
    # cannot be read.
    try:
        attributes = getattr(file_path.stat(), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_OFFLINE", 0x1000))

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
        f"iCloud download needed for {file_path.name}: "
        f"{format_size(local_disk_usage(file_path) or 0)} of "
        f"{format_size(expected_size)} is stored locally. Downloading it now..."
    )
    for attempt in range(1, HYDRATION_RETRIES + 1):
        progress = {"bytes": 0, "done": False, "error": None}

        def _read_worker():
            try:
                with open(file_path, "rb", buffering=HYDRATION_READ_SIZE) as source:
                    while True:
                        block = source.read(HYDRATION_READ_SIZE)
                        if not block:
                            break
                        progress["bytes"] += len(block)
                progress["done"] = True
            except (OSError, TimeoutError) as exc:
                progress["error"] = exc

        # The read itself cannot be interrupted once iCloud's driver blocks it,
        # so it runs in a daemon thread and a watchdog below abandons the
        # attempt when no progress is made for HYDRATION_STALL_TIMEOUT.
        worker = threading.Thread(target=_read_worker, daemon=True)
        worker.start()

        last_bytes = -1
        last_progress_time = time.time()
        last_reported_percent = -1
        stalled = False
        while worker.is_alive():
            time.sleep(15)
            if progress["bytes"] != last_bytes:
                last_bytes = progress["bytes"]
                last_progress_time = time.time()
                percent = (int(last_bytes * 100 / expected_size)
                           if expected_size else 100)
                if percent >= last_reported_percent + 5 or percent == 100:
                    logger.info(
                        f"  iCloud download... {percent}% "
                        f"({format_size(last_bytes)}/{format_size(expected_size)})"
                    )
                    last_reported_percent = percent
            elif time.time() - last_progress_time > HYDRATION_STALL_TIMEOUT:
                stalled = True
                logger.error(
                    f"  iCloud download stalled: no progress for "
                    f"{HYDRATION_STALL_TIMEOUT // 60} minutes on {file_path.name}. "
                    "Abandoning this attempt; the file can be retried later."
                )
                break

        if progress["done"] and progress["bytes"] == expected_size:
            logger.info(f"iCloud download complete: {file_path.name}")
            return True
        if stalled:
            # Do not hammer a wedged file with more attempts this run; the
            # queue continues with the other videos instead.
            raise HydrationStalled(
                f"no progress for {HYDRATION_STALL_TIMEOUT // 60} minutes"
            )
        error_desc = progress["error"] or (
            f"Read {progress['bytes']} bytes but expected {expected_size} bytes"
        )
        if attempt == HYDRATION_RETRIES:
            logger.error(
                f"Could not fully download {file_path.name} from iCloud: {error_desc}"
            )
            return False
        delay = attempt * 30
        logger.warning(
            f"iCloud download attempt {attempt}/{HYDRATION_RETRIES} failed "
            f"({error_desc}); retrying in {delay}s."
        )
        time.sleep(delay)
    return False

def wait_until_fully_downloaded(file_path: Path, timeout: int = 300) -> bool:
    """Wait until iCloud has written this one video to the local disk.

    A file is ready when every byte is on disk, or once the local data stops
    growing, in which case the download is finished off (and verified) by
    reading the file sequentially.
    """
    logger.info(f"Waiting for file to finish downloading: {file_path.name}")
    stable_iterations = 0
    last_usage = -1
    start = time.time()
    while time.time() - start < timeout:
        try:
            logical_size = file_path.stat().st_size
        except FileNotFoundError:
            logger.warning(f"File disappeared during download wait: {file_path.name}")
            return False
        usage = local_disk_usage(file_path)
        if usage is None:
            usage = logical_size
        if logical_size and usage >= logical_size:
            logger.info(
                f"File is fully downloaded ({format_size(logical_size)}): {file_path.name}"
            )
            return True
        if usage == last_usage:
            stable_iterations += 1
        else:
            stable_iterations = 0
            last_usage = usage
            logger.debug(
                f"  Local data: {format_size(usage)} of {format_size(logical_size)}"
            )
        if stable_iterations >= 3:  # unchanged for ~3 checks (~3 seconds)
            logger.info(
                f"Download is idle at {format_size(usage)} of "
                f"{format_size(logical_size)}: {file_path.name}"
            )
            return hydrate_icloud_file(file_path)
        time.sleep(1)
    logger.warning(f"Timed out ({timeout}s) waiting for the download: {file_path.name}")
    return False

class HydrationStalled(RuntimeError):
    """Raised when an iCloud hydration read makes no progress."""


def ensure_video_downloaded(file_path: Path) -> bool:
    """Download a single video from iCloud, retrying a few times first.

    Only one video is ever downloaded at a time because this is only called from
    the per-video pipeline in process_one_video().
    """
    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        try:
            if wait_until_fully_downloaded(file_path, timeout=DOWNLOAD_WAIT_TIMEOUT):
                return True
        except HydrationStalled:
            # The read is wedged inside iCloud's driver; retrying immediately
            # cannot help. Give up on this file so the queue keeps flowing.
            logger.error(
                f"{file_path.name} needs iCloud to recover (download wedged). "
                "Skipping it for this run."
            )
            return False
        if not file_path.exists():
            logger.error(f"{file_path.name} disappeared while waiting for its iCloud download.")
            return False
        if attempt < MAX_DOWNLOAD_ATTEMPTS:
            logger.warning(
                f"iCloud download attempt {attempt}/{MAX_DOWNLOAD_ATTEMPTS} was not "
                f"complete for {file_path.name}; retrying in {DOWNLOAD_RETRY_DELAY}s."
            )
            time.sleep(DOWNLOAD_RETRY_DELAY)
    logger.error(
        f"Could not download {file_path.name} after {MAX_DOWNLOAD_ATTEMPTS} attempt(s)."
    )
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
    # Network interruptions and intercepting proxies can surface as httplib2
    # redirect/transport errors mid-upload. They are transient: the resumable
    # session stays valid and the same chunk is retried after a short wait.
    if isinstance(exc, httplib2.HttpLib2Error):
        if isinstance(exc, httplib2.error.RedirectMissingLocation):
            return True
        return "redirect" in str(exc).lower() or "connection" in str(exc).lower()
    return "timed out" in str(exc).lower()

def stage_local_copy(file_path: Path) -> Optional[Path]:
    """Copy the downloaded video to a plain local folder before uploading it.

    Reading a video directly from the iCloud Drive folder can fail in the
    middle of the upload (OSError [Errno 22] when iCloud stalls or re-evicts a
    placeholder). A half-transferred upload leaves a broken video on YouTube
    that stays in "Processing" forever. A plain local copy is stable for the
    whole upload. The copy is deleted again right after the upload, so only
    one staged file is ever on disk.

    Returns the path of the staged copy, or None when staging was not
    possible (the caller then uploads from the iCloud folder directly).
    """
    staged_path = Path(STAGING_DIR) / file_path.name
    try:
        os.makedirs(STAGING_DIR, exist_ok=True)
        if staged_path.exists():
            staged_path.unlink()
        size = file_path.stat().st_size
        logger.info(f"  Staging a stable local copy ({format_size(size)}): {file_path.name}")
        copied = 0
        last_reported = -10
        with open(file_path, "rb", buffering=HYDRATION_READ_SIZE) as source, \
                open(staged_path, "wb") as destination:
            while True:
                block = source.read(HYDRATION_READ_SIZE)
                if not block:
                    break
                destination.write(block)
                copied += len(block)
                percent = int(copied * 100 / size) if size else 100
                if percent >= last_reported + 10 or percent == 100:
                    logger.info(f"  Staging... {percent}% "
                                f"({format_size(copied)} / {format_size(size)})")
                    last_reported = percent
        if copied != size:
            raise OSError(f"staged {copied} bytes but the file has {size}")
        logger.info(f"  Staging complete: {staged_path}")
        return staged_path
    except Exception as exc:
        logger.warning(f"  Could not stage {file_path.name} locally ({exc}). "
                       f"Falling back to uploading from the iCloud folder.")
        try:
            if staged_path.exists():
                staged_path.unlink()
        except OSError:
            pass
        return None


def cleanup_zombie_uploads(youtube, title: str) -> None:
    """Delete the half-finished upload a failed attempt leaves on YouTube.

    When a resumable upload dies before its final chunk, YouTube keeps a
    broken video that stays in "Processing" forever (visible in YouTube
    Studio). Such videos are identified safely by their unique reserved title
    plus an unfinalized status, so successfully processed uploads are never
    touched.
    """
    if not title or not title.startswith("Date_"):
        return
    time.sleep(90)  # give YouTube a moment to register the abandoned session
    try:
        me = youtube.channels().list(part="contentDetails", mine=True).execute()
        for channel in me.get("items", []):
            uploads_pid = channel["contentDetails"]["relatedPlaylists"]["uploads"]
            page_token = None
            while True:
                resp = youtube.playlistItems().list(
                    part="contentDetails", playlistId=uploads_pid,
                    maxResults=50, pageToken=page_token).execute()
                ids = [i["contentDetails"]["videoId"]
                       for i in resp.get("items", [])][:25]
                page_token = resp.get("nextPageToken")
                if ids:
                    details = youtube.videos().list(
                        part="status,processingDetails,snippet",
                        id=",".join(ids)).execute()
                    for video in details.get("items", []):
                        same_title = video["snippet"].get("title") == title
                        unfinalized = (
                            video["status"].get("uploadStatus") == "uploaded"
                            and video.get("processingDetails", {})
                            .get("processingStatus") == "processing"
                        )
                        if same_title and unfinalized:
                            try:
                                youtube.videos().delete(id=video["id"]).execute()
                                logger.warning(
                                    f"  Removed the abandoned upload left by the "
                                    f"failed attempt: {video['id']} ({title})")
                            except Exception as exc:
                                logger.warning(
                                    f"  Could not remove the abandoned upload "
                                    f"{video['id']}: {exc}")
                if not page_token:
                    break
    except Exception as exc:
        logger.debug(f"Zombie cleanup skipped: {exc}")


def is_resumable_session_error(exc: Exception) -> bool:
    """Errors where the resumable session itself is broken.

    Redirect errors (RedirectMissingLocation, 3xx with no Location header)
    mean the upload session URI is no longer valid, so we must start a fresh
    resumable upload rather than retrying the same broken session.
    """
    if isinstance(exc, httplib2.error.RedirectMissingLocation):
        return True
    text = str(exc).lower()
    if "redirect" in text and "location" in text:
        return True
    return False


def upload_video(youtube, file_path: Path, title: str) -> Optional[str]:
    """Uploads a video with progress logging and returns the YouTube videoId on success.

    If a redirect or transport error breaks the resumable session, a brand-new
    session is created (new MediaFileUpload + new insert request) so the upload
    can proceed from the beginning.
    """
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
    upload_start_time = time.time()
    try:
        retry_count = 0
        while True:
            # (Re)create the media upload and request object on every attempt.
            # This gives each retry a fresh resumable session, which is essential
            # after redirect/transport errors that invalidate the previous session.
            media = MediaFileUpload(str(file_path), chunksize=CHUNK_SIZE, resumable=True)
            request = youtube.videos().insert(
                part=','.join(body.keys()), body=body, media_body=media
            )

            response = None
            session_broken = False
            while response is None and not session_broken:
                try:
                    # One immediate retry is handled by the client; the loop below
                    # provides visible backoff for longer connection interruptions.
                    status, response = request.next_chunk(num_retries=1)
                except Exception as exc:
                    if is_upload_limit_error(exc):
                        raise
                    if is_resumable_session_error(exc):
                        # The resumable session is dead (e.g. redirect without a
                        # Location header). Start a completely new upload session.
                        retry_count += 1
                        if retry_count > MAX_UPLOAD_RETRIES:
                            raise
                        delay = min(300, (2 ** retry_count) + random.uniform(0, 1))
                        logger.warning(
                            f"Resumable upload session broken ({exc}). "
                            f"Starting a fresh upload in {int(delay)}s "
                            f"({retry_count}/{MAX_UPLOAD_RETRIES})."
                        )
                        time.sleep(delay)
                        session_broken = True  # signal outer loop to recreate
                        break  # break inner while -> recreate request above
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

            # If the session was broken, recreate the request (outer loop continues)
            if session_broken:
                continue

            # Inner loop exited with a valid response -> upload complete
            break

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

def process_one_video(youtube, file_path: Path, context: str = "") -> tuple:
    """Download and upload exactly one video, then clean up after it.

    The whole download -> upload -> clean-up cycle for a single file lives here,
    so only one video is ever transferred at a time. A real failure records the
    video and OFFLOADS (not deletes) the local copy before returning, which lets
    the caller move straight on to the next video while the video itself stays
    safely in iCloud. Quota errors are the one exception: the local copy is kept
    so the video can be retried after the cooldown.

    Returns (youtube, status) where status is one of:
      "uploaded"     - uploaded, added to the playlist, local copy offloaded
      "skipped"      - already uploaded, gone from disk, or nothing to do
      "failed"       - download/upload failed and the local copy was offloaded
      "quota_paused" - every account is out of quota; the file is kept
    """
    global current_account_index

    prefix = f"{context} " if context else ""
    upload_succeeded = False
    staged_path = None
    currently_processing.set()
    try:
        while True:

            if file_path.name in UPLOADED_HISTORY:
                logger.info(f"{prefix}SKIPPED (already uploaded): {file_path.name}")
                stats["skipped"] += 1
                return youtube, "skipped"

            if SKIP_FAILED_VIDEOS and file_path.name in FAILED_VIDEOS:
                logger.info(f"{prefix}SKIPPED (failed earlier): {file_path.name}")
                stats["skipped"] += 1
                return youtube, "skipped"

            if not file_path.exists():
                logger.warning(f"{prefix}SKIPPED (file is gone): {file_path.name}")
                stats["skipped"] += 1
                return youtube, "skipped"

            # Step 1: download this one video from iCloud and confirm every byte
            # is available locally before the upload starts.
            if not ensure_video_downloaded(file_path):
                logger.error(f"{prefix}DOWNLOAD FAILED: {file_path.name}")
                stats["failed"] += 1
                drop_failed_video(file_path, "download failed")
                logger.info(f"{prefix}Moving on to the next video. "
                            f"{file_path.name} stays in iCloud.")
                print_stats()
                return youtube, "failed"

            # Step 2: upload the file that was just downloaded. Whenever
            # possible the upload reads from a staged local copy OUTSIDE the
            # iCloud folder, so a stalled iCloud placeholder read ([Errno 22])
            # cannot break the upload halfway and leave a broken "stuck
            # processing" video on YouTube.
            staged_path = stage_local_copy(file_path)
            upload_target = staged_path or file_path
            title = get_or_create_title(file_path.name)
            video_id = upload_video(youtube, upload_target, title)

            if video_id == "QUOTA_EXCEEDED":
                if switch_account():
                    new_youtube = get_authenticated_service()
                    if new_youtube:
                        # Retry this same video with the fallback client.
                        youtube = new_youtube
                        continue
                    logger.error("Failed to authenticate with fallback account.")

                retry_at = time.time() + UPLOAD_LIMIT_COOLDOWN
                save_retry_after(retry_at)
                upload_limit_reached.set()
                logger.error("=" * 60)
                logger.error("ALL ACCOUNTS UPLOAD LIMIT REACHED")
                logger.error("Uploads are paused. This video is kept for a later retry.")
                logger.error(
                    "Automatic retry after approx "
                    f"{datetime.datetime.fromtimestamp(retry_at).strftime('%Y-%m-%d %H:%M:%S')}"
                )
                logger.error("=" * 60)
                current_account_index = 0
                return youtube, "quota_paused"

            if not video_id:
                # The upload failed: offload the local copy (the video stays in
                # iCloud) before the next video is started. Also remove the
                # broken half-uploaded video this attempt may have left behind.
                logger.error(f"{prefix}UPLOAD FAILED: {file_path.name}")
                drop_failed_video(file_path, "upload failed")
                cleanup_zombie_uploads(youtube, title)
                logger.info(f"{prefix}Moving on to the next video. "
                            f"{file_path.name} stays in iCloud.")
                print_stats()
                return youtube, "failed"

            # Step 3: success - playlist, history, then free the disk space.
            # The local copy is OFFLOADED (unpinned), never deleted, so the
            # video remains safely in iCloud after the upload.
            upload_succeeded = True
            add_to_playlist(youtube, video_id)
            append_history(file_path.name, title)
            logger.info(f"  Upload complete. Offloading the local copy of "
                        f"{file_path.name} - it stays in iCloud.")
            offload_local_file(file_path, "upload complete")

            print_stats()
            return youtube, "uploaded"
    except Exception as exc:
        logger.error(f"{prefix}UNEXPECTED ERROR while processing {file_path.name}: {exc}")
        logger.debug("Unexpected processing traceback:", exc_info=True)
        if upload_succeeded:
            logger.warning(f"  {file_path.name} was uploaded; no local copy is kept.")
            return youtube, "uploaded"
        stats["failed"] += 1
        drop_failed_video(file_path, f"unexpected error ({exc})")
        cleanup_zombie_uploads(youtube, get_or_create_title(file_path.name))
        print_stats()
        return youtube, "failed"
    finally:
        # Remove the staged local copy (it lives OUTSIDE iCloud, so deleting it
        # never touches iCloud itself); the video in iCloud stays untouched.
        if staged_path:
            try:
                staged_path.unlink()
                logger.debug(f"  Removed staged copy: {staged_path}")
            except OSError:
                pass
        currently_processing.clear()

def process_queue():
    """Worker thread: handle one queued video at a time."""
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
        logger.info(
            f"--- Processing: {file_path.name} "
            f"(Queue remaining: {video_queue.qsize()}) ---"
        )
        try:
            # One video at a time: the next item is only taken off the queue once
            # this video has been downloaded, uploaded, and cleaned up.
            youtube, status = process_one_video(youtube, file_path, context="[QUEUE]")
        finally:
            with queued_lock:
                queued_video_names.discard(file_path.name)
            video_queue.task_done()

        if status == "quota_paused" and file_path.exists():
            # Quota pauses are temporary, so keep the video for the automatic
            # retry once the cooldown expires.
            enqueue_video(file_path, reason="retry after upload limit cooldown")

class VideoHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            path = Path(event.src_path)
            if is_video_file(path):
                logger.info(f"[DETECTED] New video: {path.name}")
                enqueue_video(path, reason="new file detected")

    def on_moved(self, event):
        # Handle cases where iCloud finishes download by moving a temp file
        if not event.is_directory:
            path = Path(event.dest_path)
            if is_video_file(path):
                logger.info(f"[DETECTED] Moved video: {path.name}")
                enqueue_video(path, reason="download finished")

def scan_existing_videos() -> list:
    """Scan the folder for existing video files and return sorted list."""
    folder = Path(ICLOUD_FOLDER)
    all_files = [f for f in folder.iterdir() if f.is_file()]
    
    videos = []
    skipped_non_video = []
    skipped_already_uploaded = []
    skipped_failed = []

    for f in all_files:
        if not is_video_file(f):
            skipped_non_video.append(f)

        elif f.name in UPLOADED_HISTORY:
            skipped_already_uploaded.append(f)
        elif SKIP_FAILED_VIDEOS and f.name in FAILED_VIDEOS:
            skipped_failed.append(f)
        else:

            videos.append(f)
            
    videos.sort(key=lambda f: f.stat().st_mtime)  # oldest first

    logger.info(f"Folder scan: {len(all_files)} total files found")
    logger.info(f"  To upload: {len(videos)} ({', '.join(VIDEO_EXTENSIONS)})")
    logger.info(f"  Skipped (non-video): {len(skipped_non_video)}")

    logger.info(f"  Skipped (already uploaded): {len(skipped_already_uploaded)}")
    logger.info(f"  Skipped (failed earlier): {len(skipped_failed)}")

    if skipped_non_video:
        for f in skipped_non_video:
            logger.debug(f"    Skipped (non-video): {f.name}")

    if skipped_already_uploaded:
        for f in skipped_already_uploaded:
            logger.debug(f"    Skipped (uploaded): {f.name}")
    if skipped_failed:
        for f in skipped_failed:
            logger.debug(f"    Skipped (failed earlier): {f.name}")

    return videos

def bulk_upload_existing(youtube):
    """Upload all existing videos in the folder before starting the watcher.

    Videos are handled strictly one at a time: each file is downloaded from
    iCloud, uploaded, and removed from disk before the next one starts.
    """
    existing = scan_existing_videos()
    if not existing:
        logger.info("No existing videos found in folder. Skipping bulk upload.")
        return youtube

    logger.info("=" * 50)
    logger.info(f"BULK UPLOAD PHASE — {len(existing)} video(s) to process")
    logger.info("Videos are processed one at a time (download → upload → clean up).")
    logger.info("=" * 50)

    for i, file_path in enumerate(existing, 1):
        logger.info(f"\n--- Bulk upload [{i}/{len(existing)}]: {file_path.name} ---")
        youtube, status = process_one_video(
            youtube, file_path, context=f"[{i}/{len(existing)}]"
        )
        if status == "quota_paused":
            if file_path.exists():
                # Quota pauses are temporary, so keep the video for the retry.
                enqueue_video(file_path, reason="retry after upload limit cooldown")
            break

    logger.info("=" * 50)
    logger.info("BULK UPLOAD PHASE COMPLETE")
    print_stats()
    logger.info("=" * 50)
    return youtube

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
    logger.info("  Policy: one video at a time; failed videos are offloaded back "
                "to iCloud, never deleted.")
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
    youtube = bulk_upload_existing(youtube)

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

            logger.info("Scanning folder for videos that still need uploading...")

            logger.info("="*50)
            
            # scan_existing_videos automatically excludes successfully uploaded files
            retry_videos = scan_existing_videos()
            if retry_videos:
                logger.info(f"Queuing {len(retry_videos)} video(s) for retry.")
                for file_path in retry_videos:
                    enqueue_video(file_path, reason="24-hour retry scan")
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
