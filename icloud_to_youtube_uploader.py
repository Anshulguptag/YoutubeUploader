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
- Tracks uploaded files and SHA-256 content hashes in uploaded_history.txt to
  avoid duplicates even when a video is renamed

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
import hashlib

from ctypes import wintypes
from pathlib import Path

from typing import Optional, Set

import httplib2
from google_auth_httplib2 import AuthorizedHttp

# httplib2 treats every 308 response as a web redirect and raises
# RedirectMissingLocation when YouTube's resumable endpoint correctly returns
# 308 Resume Incomplete without a Location header.  For a PUT that has a
# Content-Range header, this is not a redirect: googleapiclient needs the 308
# response (and its Range header) to continue at the acknowledged byte.  Keep
# this compatibility shim deliberately narrow so ordinary redirects still
# raise normally.
_ORIGINAL_HTTPLIB2_REQUEST = httplib2.Http.request


def _resumable_308_request(self, uri, method="GET", body=None, headers=None,
                           *args, **kwargs):
    try:
        return _ORIGINAL_HTTPLIB2_REQUEST(
            self, uri, method=method, body=body, headers=headers, *args, **kwargs
        )
    except httplib2.error.RedirectMissingLocation as exc:
        response = getattr(exc, "response", None)
        status = getattr(response, "status", None)
        is_resumable_put = (
            method.upper() == "PUT" and
            any(key.lower() == "content-range" for key in (headers or {}))
        )
        if status == 308 and is_resumable_put:
            return response, getattr(exc, "content", b"")
        raise


httplib2.Http.request = _resumable_308_request



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
    os.path.join(SCRIPT_DIR, "client_secret_3.json"),
]
TOKEN_FILES = [
    os.path.join(SCRIPT_DIR, "token.json"),
    os.path.join(SCRIPT_DIR, "token_2.json"),
    os.path.join(SCRIPT_DIR, "token_3.json"),
]
# Playlist ID where videos should be added (replace with your own)
PLAYLIST_ID = "PLKwxP3c9cudg"
# Upload chunk size must be a multiple of 256 KB for resumable YouTube uploads.
# Smaller chunks make retries less expensive when a slow connection drops.
# 8 MiB keeps the request count reasonable for a 20 GB file (2,560 chunks)
# while still making an individual retry inexpensive.  YouTube requires a
# multiple of 256 KiB for every non-final resumable chunk.
CHUNK_SIZE = 256 * 1024 * 32  # 8 MiB
UPLOAD_HTTP_TIMEOUT = 10 * 60  # Large iCloud videos can take longer than 60 seconds per request.
# Transient transfer failures are retried for as long as the process remains
# running. A 20 GB upload should not be marked failed merely because a home
# connection drops repeatedly; permanent 4xx API errors still stop promptly.
MAX_RETRY_DELAY = 15 * 60
PROCESSING_POLL_INTERVAL = 60
# A video is not considered successful until YouTube has finished processing
# it. If it is still not complete by this deadline, delete it rather than
# leaving an unverified, potentially broken video in the account or playlist.
PROCESSING_TIMEOUT = 24 * 60 * 60
ZOMBIE_PROCESSING_AGE_HOURS = 48
TITLE_PATTERN = re.compile(r"^Date_(\d{8})_(\d+)$")
# A large sequential read asks iCloud for the entire cloud-only file before
# upload; it avoids random seeks into an unavailable placeholder.
HYDRATION_READ_SIZE = 16 * 1024 * 1024  # 16 MB
HYDRATION_RETRIES = 3
# A large cloud-only video can spend several minutes establishing an iCloud
# transfer before its first read returns. Keep waiting long enough for a slow
# multi-GB download, while still eventually escaping a genuinely wedged driver.
HYDRATION_STALL_TIMEOUT = 45 * 60
MAX_YOUTUBE_FILE_SIZE = 256 * 1024 * 1024 * 1024

# Videos are processed strictly one at a time: download from iCloud, upload,
# then clean up. These settings control that single-video pipeline.
DOWNLOAD_WAIT_TIMEOUT = 5 * 60  # Seconds to wait for iCloud to finish a file.
MAX_DOWNLOAD_ATTEMPTS = 3       # Download tries before a video counts as failed.
DOWNLOAD_RETRY_DELAY = 30       # Seconds between download attempts.
# iCloud for Windows hydrates a cloud-only file onto the volume containing the
# iCloud Drive folder.  A resumable YouTube upload can seek/retry, so it needs
# that complete local source; it cannot safely upload a 60 GB placeholder from
# a 30 GB volume.  Keep this much unused space after hydration so Windows and
# iCloud do not run out of working room.  Set to 0 only when the volume is
# dedicated to the upload and you understand the risk.
HYDRATION_FREE_SPACE_RESERVE = 2 * 1024 * 1024 * 1024  # 2 GiB
CONTENT_HASH_READ_SIZE = 16 * 1024 * 1024  # 16 MiB

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
# Resolved YouTube channel labels, keyed by the configured OAuth-account slot.
# They are included in newly uploaded videos' descriptions for traceability.
UPLOAD_ACCOUNT_LABELS = {}
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

def load_uploaded_history_records() -> dict:
    """Load history records in both the current and pre-hash formats.

    Current records are ``SHA256 : original filename : Date_title``.  The
    older ``original filename : Date_title`` format is intentionally accepted
    so updating the script never loses the duplicate protection already built
    up in an existing history file.
    """
    records = {}
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                fields = [field.strip() for field in line.rstrip("\n").split(" : ", 2)]
                if len(fields) < 2 or not fields[0]:
                    continue
                if len(fields) == 3 and re.fullmatch(r"[0-9a-fA-F]{64}", fields[0]):
                    content_hash, filename, title = fields
                else:
                    # Legacy record: filename : title
                    content_hash = None
                    filename, title = fields[0], " : ".join(fields[1:])
                if filename:
                    records[filename] = {"hash": content_hash, "title": title}
    return records


UPLOADED_RECORDS = load_uploaded_history_records()
UPLOADED_HISTORY = set(UPLOADED_RECORDS)
UPLOADED_TITLE_MAP = {
    filename: record["title"] for filename, record in UPLOADED_RECORDS.items()
}
# A content hash is the duplicate key.  The filename set above remains so
# legacy records and already-known files continue to work without a rehash.
UPLOADED_CONTENT_HASHES = {
    record["hash"].lower() for record in UPLOADED_RECORDS.values()
    if record["hash"]
}
UPLOADED_HASH_TITLES = {
    record["hash"].lower(): record["title"] for record in UPLOADED_RECORDS.values()
    if record["hash"]
}

def load_failed_videos() -> set:
    """Return permanent failures; iCloud download failures remain retryable."""
    failed = set()
    if os.path.exists(FAILED_FILE):
        with open(FAILED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                entry = line.strip()
                if entry:
                    # A cloud-only placeholder can be temporarily unavailable
                    # even though the file is healthy. Do not let a past
                    # iCloud timeout permanently suppress a large video.
                    if " : download failed :" not in entry:
                        failed.add(entry.split(" : ")[0].strip())
    return failed

FAILED_VIDEOS = load_failed_videos() if SKIP_FAILED_VIDEOS else set()

def save_uploaded_history() -> None:
    """Persist the in-memory records, placing the hash before each filename."""
    temporary_file = f"{HISTORY_FILE}.tmp"
    with open(temporary_file, "w", encoding="utf-8") as f:
        for filename, record in UPLOADED_RECORDS.items():
            content_hash = record.get("hash")
            title = record.get("title", "")
            if content_hash:
                f.write(f"{content_hash} : {filename} : {title}\n")
            else:
                # A source absent from iCloud cannot be safely hashed. Retain
                # its legacy record until it becomes available again.
                f.write(f"{filename} : {title}\n")
    os.replace(temporary_file, HISTORY_FILE)


def append_history(filename: str, title: str, content_hash: Optional[str] = None):
    """Record an upload, using its SHA-256 before its original filename."""
    normalized_hash = content_hash.lower() if content_hash else None
    UPLOADED_RECORDS[filename] = {"hash": normalized_hash, "title": title}
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        if normalized_hash:
            f.write(f"{normalized_hash} : {filename} : {title}\n")
        else:
            f.write(f"{filename} : {title}\n")
    UPLOADED_HISTORY.add(filename)
    UPLOADED_TITLE_MAP[filename] = title
    if normalized_hash:
        UPLOADED_CONTENT_HASHES.add(normalized_hash)
        UPLOADED_HASH_TITLES.setdefault(normalized_hash, title)

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


def defer_icloud_download(file_path: Path) -> None:
    """Release a temporarily unavailable cloud file without blacklisting it."""
    offload_local_file(file_path, "iCloud download deferred")
    logger.warning(
        f"{file_path.name} will be retried on the next scheduled folder scan; "
        "it was not added to failed_videos.txt."
    )


def release_already_uploaded_copy(file_path: Path) -> None:
    """Free an uploaded video's local data without deleting its iCloud file.

    The uploaded-history filename is the duplicate key used by this uploader.
    `offload_local_file` uses iCloud's "Free up space" operation, so this
    leaves the item visible in iCloud Drive as a cloud-only placeholder.  It
    must never use unlink/remove: those would delete the iCloud original.
    """
    if file_path.name not in UPLOADED_HISTORY:
        return
    offload_local_file(file_path, "already uploaded")

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


def has_space_to_hydrate(file_path: Path) -> bool:
    """Check that iCloud can safely make the entire source file local.

    This intentionally runs *before* opening a cloud-only file.  Without it,
    asking iCloud to read a 60 GB placeholder on a 30 GB disk can leave a large
    partial download behind and make Windows unstable.  YouTube's API accepts
    a byte stream from this computer, not an iCloud Drive URL, and the iCloud
    Windows provider has no supported server-to-server upload path.
    """
    try:
        logical_size = file_path.stat().st_size
        cached_size = local_disk_usage(file_path) or 0
        free_size = shutil.disk_usage(file_path).free
    except OSError as exc:
        logger.error(f"Cannot check free space for {file_path.name}: {exc}")
        return False

    missing_size = max(0, logical_size - cached_size)
    required_free = missing_size + HYDRATION_FREE_SPACE_RESERVE
    if free_size >= required_free:
        return True

    logger.error(
        f"Insufficient local space to hydrate {file_path.name}: iCloud needs "
        f"another {format_size(missing_size)} and this uploader reserves "
        f"{format_size(HYDRATION_FREE_SPACE_RESERVE)} of working space. "
        f"Available: {format_size(free_size)}; required: "
        f"{format_size(required_free)}."
    )
    logger.error(
        "Do not start this upload on this volume. Move the iCloud Drive folder "
        "or download this file to an external drive with enough free space, "
        "then run the uploader again. The file remains safely in iCloud."
    )
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

        last_observed_bytes = -1
        last_progress_time = time.time()
        last_reported_percent = -1
        stalled = False
        while worker.is_alive():
            time.sleep(15)
            # iCloud can download into its local cache while a blocking read
            # has not returned a complete 16 MB block yet. Watching only
            # progress["bytes"] incorrectly calls that healthy large-file
            # transfer "stalled". Count both the reader and actual disk use.
            cached_bytes = local_disk_usage(file_path) or 0
            observed_bytes = min(expected_size, max(progress["bytes"], cached_bytes))
            if observed_bytes != last_observed_bytes:
                last_observed_bytes = observed_bytes
                last_progress_time = time.time()
                percent = (int(observed_bytes * 100 / expected_size)
                           if expected_size else 100)
                if percent >= last_reported_percent + 5 or percent == 100:
                    logger.info(
                        f"  iCloud download... {percent}% "
                        f"({format_size(observed_bytes)}/{format_size(expected_size)})"
                    )
                    last_reported_percent = percent
                if cached_bytes >= expected_size:
                    logger.info(f"iCloud download complete: {file_path.name}")
                    return True
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
    # A fully local file does not need additional free space.  A cloud-only or
    # partially hydrated file does, and the check must happen before iCloud is
    # asked to read more data.
    if is_icloud_placeholder(file_path) and not has_space_to_hydrate(file_path):
        return False

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


def content_sha256(file_path: Path) -> Optional[str]:
    """Return a file's SHA-256 digest after safely hydrating it from iCloud."""
    if not ensure_video_downloaded(file_path):
        logger.error(f"Cannot hash {file_path.name}; it was left unchanged.")
        return None
    return sha256_of_downloaded_file(file_path)


def sha256_of_downloaded_file(file_path: Path) -> Optional[str]:
    """Hash a file which the caller has already confirmed is fully local."""
    digest = hashlib.sha256()
    try:
        with open(file_path, "rb", buffering=CONTENT_HASH_READ_SIZE) as source:
            while True:
                block = source.read(CONTENT_HASH_READ_SIZE)
                if not block:
                    break
                digest.update(block)
        return digest.hexdigest()
    except OSError as exc:
        logger.error(f"Could not hash {file_path.name}: {exc}")
        return None


def backfill_uploaded_video_hashes(folder: Path) -> int:
    """Add hashes to old history records whose iCloud source is still present.

    Only already-uploaded filenames lacking a hash are considered.  They are
    hydrated and offloaded one at a time, just like an upload, so the backfill
    does not keep a collection of old videos on disk.  Missing source files
    remain as legacy filename records: their historical duplicate protection is
    preserved, but a hash cannot be invented without the original bytes.
    """
    pending = [
        (filename, record) for filename, record in UPLOADED_RECORDS.items()
        if not record.get("hash")
    ]
    if not pending:
        logger.info("Uploaded-history hash backfill: all available records already have hashes.")
        return 0

    logger.info(f"Uploaded-history hash backfill: checking {len(pending)} existing upload(s).")
    updated = 0
    missing = 0
    for filename, record in pending:
        file_path = folder / filename
        if not file_path.is_file():
            missing += 1
            logger.warning(
                f"Cannot backfill hash for {filename}: the original is not in the iCloud folder."
            )
            continue
        digest = content_sha256(file_path)
        # Hashing can hydrate a cloud-only file.  This is safe because the
        # source was already uploaded; immediately release its local data.
        offload_local_file(file_path, "uploaded-history hash backfill")
        if not digest:
            continue
        record["hash"] = digest.lower()
        UPLOADED_CONTENT_HASHES.add(digest.lower())
        UPLOADED_HASH_TITLES.setdefault(digest.lower(), record.get("title", ""))
        updated += 1
        # Persist every completed item immediately. A large iCloud backfill can
        # be stopped or interrupted, and already-calculated hashes must not be
        # lost just because later videos are still downloading.
        save_uploaded_history()
        logger.info(
            f"  Saved SHA-256 history entry for {filename}: {digest.lower()}"
        )
    logger.info(
        f"Uploaded-history hash backfill complete: {updated} hash(es) stored"
        f"; {missing} source file(s) unavailable."
    )
    return updated


def delete_content_duplicates(folder: Path) -> int:
    """Permanently delete newer, byte-identical direct children of *folder*.

    This routine is intentionally only called by --dedupe-icloud-delete. It
    first groups by size and then compares SHA-256 digests, so equal names or
    equal sizes alone never cause deletion. The oldest file is retained. Each
    source is offloaded after hashing, which limits the local iCloud cache to
    one file at a time. A file too large for the available disk is skipped.
    """
    snapshots = []
    for file_path in folder.iterdir():
        if not file_path.is_file():
            continue
        try:
            info = file_path.stat()
            snapshots.append((file_path, info.st_size, info.st_mtime_ns))
        except OSError as exc:
            logger.warning(f"Skipping unreadable folder entry {file_path.name}: {exc}")

    by_size = {}
    for entry in snapshots:
        by_size.setdefault(entry[1], []).append(entry)

    deleted = 0
    duplicate_pairs = 0
    for size, candidates in by_size.items():
        if len(candidates) < 2:
            continue
        # Keep the oldest source deterministically if the content matches.
        candidates.sort(key=lambda entry: (entry[2], entry[0].name.lower()))
        retained_by_hash = {}
        for file_path, expected_size, expected_mtime in candidates:
            digest = content_sha256(file_path)
            try:
                current = file_path.stat()
            except OSError as exc:
                logger.warning(f"Skipping changed/missing file {file_path.name}: {exc}")
                continue
            finally:
                # Hashing hydrates iCloud files; release that cache before the
                # next candidate, including when this candidate will be kept.
                offload_local_file(file_path, "duplicate content scan")

            if (current.st_size, current.st_mtime_ns) != (expected_size, expected_mtime):
                logger.warning(f"Skipping changed file during duplicate scan: {file_path.name}")
                continue
            if digest is None:
                continue
            original = retained_by_hash.get(digest)
            if original is None:
                retained_by_hash[digest] = file_path
                continue

            try:
                # The source was checked again above, immediately before this
                # irreversible operation. unlink removes it from iCloud too.
                file_path.unlink()
                deleted += 1
                duplicate_pairs += 1
                logger.warning(
                    f"Deleted duplicate from iCloud: {file_path.name} "
                    f"(kept {original.name}; {format_size(size)})"
                )
            except OSError as exc:
                logger.error(f"Could not delete duplicate {file_path.name}: {exc}")

    logger.warning(
        f"Duplicate cleanup complete: deleted {deleted} file(s) from iCloud "
        f"across {duplicate_pairs} exact-match pair(s)."
    )
    return deleted

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


def is_expired_resumable_session(exc: Exception) -> bool:
    """Return True only when YouTube says this upload URI no longer exists."""
    return isinstance(exc, HttpError) and exc.resp.status in {404, 410}


def upload_retry_delay(retry_count: int) -> float:
    """Bounded exponential backoff with jitter for long-running uploads."""
    return min(MAX_RETRY_DELAY, (2 ** min(retry_count, 10)) + random.uniform(0, 1))

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
        size = file_path.stat().st_size
        free_bytes = shutil.disk_usage(STAGING_DIR).free
        if free_bytes < size:
            logger.warning(
                f"  Only {format_size(free_bytes)} is free for staging, but "
                f"{format_size(size)} is required. Uploading the fully hydrated "
                "iCloud file directly instead."
            )
            return None
        if staged_path.exists():
            staged_path.unlink()
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
            destination.flush()
            os.fsync(destination.fileno())
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
    """Keep failed resumable attempts non-destructive.

    A resumable session is not a published YouTube video.  Deleting by title
    after an interrupted transfer risks deleting a real video with the same
    title, so recovery relies on the session-status protocol instead.
    """
    logger.debug("No remote cleanup is needed for an unfinished resumable upload.")


def upload_account_label(youtube) -> str:
    """Return a stable human-readable label for the active upload account."""
    account_number = current_account_index + 1
    cached = UPLOAD_ACCOUNT_LABELS.get(account_number)
    if cached:
        return cached

    fallback = f"OAuth account {account_number}"
    try:
        response = youtube.channels().list(part="snippet", mine=True).execute()
        channel = next(iter(response.get("items", [])), None)
        if not channel:
            logger.warning(f"Could not identify the channel for {fallback}.")
            return fallback
        channel_name = channel.get("snippet", {}).get("title") or "Unnamed channel"
        channel_id = channel.get("id", "unknown channel ID")
        label = f"{channel_name} ({fallback}; channel ID: {channel_id})"
        UPLOAD_ACCOUNT_LABELS[account_number] = label
        return label
    except Exception as exc:
        logger.warning(f"Could not identify the channel for {fallback}: {exc}")
        return fallback


def upload_video(youtube, file_path: Path, title: str) -> Optional[str]:
    """Uploads a video with progress logging and returns the YouTube videoId on success.

    Transport errors are retried on the same request.  googleapiclient then
    performs the resumable protocol's status query and continues from the last
    byte YouTube confirmed, rather than retransmitting a 20 GB file from zero.
    A fresh session is created only after a 404/410 confirms that the old
    session has expired.
    """
    file_size = file_path.stat().st_size
    if file_size <= 0:
        raise ValueError(f"Cannot upload an empty file: {file_path.name}")
    if file_size > MAX_YOUTUBE_FILE_SIZE:
        raise ValueError(
            f"{file_path.name} is {format_size(file_size)}, above YouTube's "
            f"{format_size(MAX_YOUTUBE_FILE_SIZE)} per-file limit."
        )
    logger.info(f"{'='*50}")
    logger.info(f"UPLOAD STARTED")
    logger.info(f"  File:  {file_path.name}")
    logger.info(f"  Size:  {format_size(file_size)}")
    logger.info(f"  Title: {title}")
    logger.info(f"{'='*50}")

    account_label = upload_account_label(youtube)
    logger.info(f"  Upload account: {account_label}")
    body = {
        'snippet': {
            'title': title,
            'description': (
                f'Uploaded automatically on {datetime.datetime.now().isoformat()}\n'
                f'Uploaded by: {account_label}'
            ),
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
        session_restarts = 0
        while True:
            # Keep this request alive across transient failures.  Its internal
            # _in_error_state tells next_chunk() to ask YouTube for the accepted
            # range before sending another byte.
            media = MediaFileUpload(str(file_path), chunksize=CHUNK_SIZE, resumable=True)
            request = youtube.videos().insert(
                part=','.join(body.keys()), body=body, media_body=media
            )

            response = None
            while response is None:
                try:
                    status, response = request.next_chunk(num_retries=0)
                except Exception as exc:
                    if is_upload_limit_error(exc):
                        raise
                    if is_expired_resumable_session(exc):
                        session_restarts += 1
                        delay = upload_retry_delay(session_restarts)
                        logger.warning(
                            f"YouTube expired the resumable session ({exc}). "
                            f"Starting a new session in {int(delay)}s "
                            f"(restart {session_restarts}; it will keep retrying)."
                        )
                        time.sleep(delay)
                        break
                    if not is_transient_upload_error(exc):
                        raise
                    retry_count += 1
                    delay = upload_retry_delay(retry_count)
                    logger.warning(
                        f"Temporary upload error ({exc}). Querying the existing "
                        f"resumable session and retrying in {int(delay)}s "
                        f"(attempt {retry_count}; it will keep retrying)."
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

            # A 404/410 ended the session; all other transient errors stay in
            # the same request and are resumed by the inner loop above.
            if response is None:
                continue
            break

        elapsed_total = time.time() - upload_start_time
        video_id = response.get('id')
        avg_speed = file_size / elapsed_total if elapsed_total > 0 else 0
        logger.info("  Uploading... 100% - COMPLETE")
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
        return True
    except Exception as e:
        logger.error(f"  Failed to add to playlist: {e}")
        return False


def online_playlist_video_ids(youtube) -> Set[str]:
    """Read the actual playlist membership from YouTube, never local files."""
    video_ids: Set[str] = set()
    page_token = None
    while True:
        response = youtube.playlistItems().list(
            part="contentDetails", playlistId=PLAYLIST_ID,
            maxResults=50, pageToken=page_token,
        ).execute()
        video_ids.update(
            item.get("contentDetails", {}).get("videoId")
            for item in response.get("items", [])
            if item.get("contentDetails", {}).get("videoId")
        )
        page_token = response.get("nextPageToken")
        if not page_token:
            return video_ids


def uploader_services(current_youtube) -> list:
    """Return each configured account without changing the active uploader."""
    global current_account_index
    services = [current_youtube]
    original_index = current_account_index
    try:
        for account_index in range(len(CLIENT_SECRETS_FILES)):
            if account_index == original_index:
                continue
            current_account_index = account_index
            service = get_authenticated_service()
            if service:
                services.append(service)
    finally:
        current_account_index = original_index
    return services


def playlist_owner_service(current_youtube):
    """Prefer the primary account for playlist reconciliation after fallback use."""
    global current_account_index
    if current_account_index == 0:
        return current_youtube
    original_index = current_account_index
    try:
        current_account_index = 0
        return get_authenticated_service() or current_youtube
    finally:
        current_account_index = original_index


def processed_uploader_video_ids(youtube) -> Set[str]:
    """Return completed Date_ uploader videos owned by one authenticated account."""
    candidates: Set[str] = set()
    try:
        channels = youtube.channels().list(part="contentDetails", mine=True).execute()
        for channel in channels.get("items", []):
            uploads_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]
            page_token = None
            while True:
                response = youtube.playlistItems().list(
                    part="snippet,contentDetails", playlistId=uploads_id,
                    maxResults=50, pageToken=page_token,
                ).execute()
                for item in response.get("items", []):
                    title = item.get("snippet", {}).get("title", "")
                    video_id = item.get("contentDetails", {}).get("videoId")
                    if video_id and TITLE_PATTERN.fullmatch(title):
                        candidates.add(video_id)
                page_token = response.get("nextPageToken")
                if not page_token:
                    break
    except Exception as exc:
        logger.warning(f"Could not read an account's uploaded videos: {exc}")
        return set()

    completed: Set[str] = set()
    for start in range(0, len(candidates), 50):
        batch = list(candidates)[start:start + 50]
        try:
            response = youtube.videos().list(
                part="status,processingDetails", id=",".join(batch)
            ).execute()
        except Exception as exc:
            logger.warning(f"Could not verify uploaded-video processing status: {exc}")
            continue
        for video in response.get("items", []):
            status = video.get("status", {}).get("uploadStatus")
            processing = video.get("processingDetails", {}).get("processingStatus")
            if status == "processed" or processing == "succeeded":
                completed.add(video["id"])
    return completed


def reconcile_online_playlist(youtube, reason: str) -> int:
    """Add completed uploader videos missing from the live target playlist.

    This deliberately does not inspect uploaded_history.txt or other local
    bookkeeping. The playlist itself is the source of truth after a restart,
    sleep, crash, or interrupted playlist insertion.
    """
    try:
        present = online_playlist_video_ids(youtube)
    except Exception as exc:
        logger.warning(f"Could not read the online playlist for reconciliation: {exc}")
        return 0

    missing: Set[str] = set()
    for service in uploader_services(youtube):
        missing.update(processed_uploader_video_ids(service) - present)
    if not missing:
        logger.info(f"Online playlist reconciliation ({reason}): no missing videos.")
        return 0

    logger.warning(
        f"Online playlist reconciliation ({reason}): adding {len(missing)} "
        "completed uploader video(s) missing from YouTube playlist."
    )
    added = 0
    for video_id in sorted(missing):
        if add_to_playlist(youtube, video_id):
            added += 1
            present.add(video_id)
    logger.info(f"Online playlist reconciliation complete: added {added} video(s).")
    return added


def set_video_recording_date(youtube, video_id: str, file_path: Path) -> None:
    """Store the Windows creation time as the video's recording date.

    This makes the playlist order independent of the order in which iCloud
    happens to make files available for upload. Existing videos without this
    metadata are handled by resequence_playlist_by_creation.py using their
    original local source file where it is still available.
    """
    try:
        created = datetime.datetime.fromtimestamp(
            file_path.stat().st_ctime, tz=datetime.timezone.utc
        )
        recording_date = created.isoformat(timespec="seconds").replace("+00:00", "Z")
        youtube.videos().update(
            part="recordingDetails",
            body={"id": video_id, "recordingDetails": {"recordingDate": recording_date}},
        ).execute()
        logger.info(f"  Saved creation date for playlist ordering: {recording_date}")
    except Exception as exc:
        # A completed video stays valid even if metadata saving is unavailable.
        # The resequencing script can fall back to its local source timestamp.
        logger.warning(f"  Could not save the creation date for {video_id}: {exc}")


def delete_youtube_video(youtube, video_id: str, reason: str) -> bool:
    """Permanently delete only the video ID produced by this account/API call."""
    try:
        youtube.videos().delete(id=video_id).execute()
        logger.warning(f"  Deleted YouTube video {video_id} ({reason}).")
        return True
    except Exception as exc:
        logger.error(f"  Could not delete YouTube video {video_id}: {exc}")
        return False


def wait_for_video_processing(youtube, video_id: str) -> bool:
    """Wait for YouTube processing, deleting a failed or timed-out upload.

    Upload completion only confirms receipt of all bytes. This waits for
    YouTube to confirm processing before the video is added to the playlist.
    Consequently a processing failure never becomes a playlist zombie.
    """
    deadline = time.time() + PROCESSING_TIMEOUT
    while time.time() < deadline:
        try:
            response = youtube.videos().list(
                part="status,processingDetails", id=video_id
            ).execute()
            video = next(iter(response.get("items", [])), None)
            if not video:
                logger.error(f"  YouTube did not return the uploaded video {video_id}.")
                delete_youtube_video(youtube, video_id, "could not verify processing")
                return False
            status = video.get("status", {})
            processing = video.get("processingDetails", {})
            upload_status = status.get("uploadStatus", "unknown")
            processing_status = processing.get("processingStatus", "processing")
            if upload_status in {"failed", "rejected", "deleted"} or \
                    processing_status in {"failed", "terminated"}:
                reason = processing.get("failureReason") or upload_status
                logger.error(f"  YouTube processing failed for {video_id}: {reason}")
                delete_youtube_video(youtube, video_id, f"processing failed: {reason}")
                return False
            if processing_status == "succeeded" or upload_status == "processed":
                logger.info(f"  YouTube processing complete: {video_id}")
                return True
            logger.info(
                f"  Waiting for YouTube processing: {video_id} "
                f"(upload={upload_status}, processing={processing_status})"
            )
        except Exception as exc:
            logger.warning(f"  Could not check YouTube processing for {video_id}: {exc}")
        time.sleep(PROCESSING_POLL_INTERVAL)

    logger.error(
        f"  YouTube processing did not finish within "
        f"{PROCESSING_TIMEOUT // 3600} hour(s): {video_id}"
    )
    delete_youtube_video(youtube, video_id, "processing timed out")
    return False


def _published_before(timestamp: str, cutoff: datetime.datetime) -> bool:
    """Return whether an RFC 3339 timestamp is older than the cutoff."""
    try:
        published = datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return published <= cutoff
    except (AttributeError, ValueError):
        return False


def remove_zombie_videos_from_playlist(youtube, stuck_after_hours: int) -> int:
    """Remove confirmed broken videos from the configured playlist and channel.

    A video is removed only if YouTube explicitly reports it failed/rejected or
    it has remained in processing longer than ``stuck_after_hours``. Playlist
    entries whose video has already disappeared are removed from the playlist.
    """
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        hours=stuck_after_hours
    )
    playlist_items = []
    page_token = None
    while True:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails", playlistId=PLAYLIST_ID,
            maxResults=50, pageToken=page_token,
        ).execute()
        playlist_items.extend(response.get("items", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    removed = 0
    for start in range(0, len(playlist_items), 50):
        batch = playlist_items[start:start + 50]
        ids = [item.get("contentDetails", {}).get("videoId") for item in batch]
        ids = [video_id for video_id in ids if video_id]
        # processingDetails is owner-only. Read the broadly available status
        # in batches first, then request owner-only processing data one video
        # at a time. A playlist can include videos from other channels.
        details = youtube.videos().list(
            part="status", id=",".join(ids)
        ).execute().get("items", []) if ids else []
        by_id = {video["id"]: video for video in details}
        for item in batch:
            playlist_item_id = item["id"]
            video_id = item.get("contentDetails", {}).get("videoId")
            video = by_id.get(video_id)
            if video is not None:
                try:
                    owner_details = youtube.videos().list(
                        part="status,processingDetails", id=video_id
                    ).execute().get("items", [])
                    if owner_details:
                        video = owner_details[0]
                except HttpError as exc:
                    if exc.resp.status != 403:
                        raise
                    logger.debug(
                        f"Cannot inspect owner-only processing details for "
                        f"{video_id}; leaving it unchanged."
                    )
            status = (video or {}).get("status", {})
            processing = (video or {}).get("processingDetails", {})
            upload_status = status.get("uploadStatus")
            processing_status = processing.get("processingStatus")
            published_at = item.get("snippet", {}).get("publishedAt", "")
            missing = video is None
            failed = upload_status in {"failed", "rejected", "deleted"} or \
                processing_status in {"failed", "terminated"}
            stuck = processing_status == "processing" and _published_before(
                published_at, cutoff
            )
            if not (missing or failed or stuck):
                continue
            reason = "missing video" if missing else (
                "processing stalled" if stuck else "YouTube marked it failed"
            )
            logger.warning(f"Removing zombie playlist item {video_id}: {reason}")
            try:
                youtube.playlistItems().delete(id=playlist_item_id).execute()
                if not missing:
                    delete_youtube_video(youtube, video_id, reason)
                removed += 1
            except Exception as exc:
                logger.error(f"Could not remove zombie {video_id}: {exc}")
    logger.info(f"Zombie cleanup complete: removed {removed} playlist item(s).")
    return removed

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
                release_already_uploaded_copy(file_path)
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
                defer_icloud_download(file_path)
                logger.info(f"{prefix}Moving on to the next video. "
                            f"{file_path.name} stays in iCloud.")
                print_stats()
                return youtube, "failed"

            # The name is not a reliable duplicate key: iCloud users can copy
            # or rename the same video.  Hash only after hydration so SHA-256
            # always represents the complete source bytes.
            content_hash = sha256_of_downloaded_file(file_path)
            if not content_hash:
                logger.error(f"{prefix}HASH FAILED: {file_path.name}")
                stats["failed"] += 1
                defer_icloud_download(file_path)
                print_stats()
                return youtube, "failed"
            content_hash = content_hash.lower()
            if content_hash in UPLOADED_CONTENT_HASHES:
                existing_title = UPLOADED_HASH_TITLES.get(content_hash, "")
                logger.info(
                    f"{prefix}SKIPPED (same SHA-256 content already uploaded): "
                    f"{file_path.name}"
                )
                # Remember this alternate original name too. Future scans can
                # skip it without hydrating it, while the hash still catches
                # additional renamed copies.
                append_history(file_path.name, existing_title, content_hash)
                offload_local_file(file_path, "duplicate content already uploaded")
                stats["skipped"] += 1
                return youtube, "skipped"

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

            # Step 3: YouTube has received every byte, so do not block the
            # single-file pipeline waiting for its separate processing queue.
            # Add the video and start the next file immediately.
            # Step 4: success - playlist, history, then free the disk space.
            # The local copy is OFFLOADED (unpinned), never deleted, so the
            # video remains safely in iCloud after the upload.
            upload_succeeded = True
            set_video_recording_date(youtube, video_id, file_path)
            add_to_playlist(youtube, video_id)
            append_history(file_path.name, title, content_hash)
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
            reconcile_online_playlist(
                playlist_owner_service(youtube), "upload-limit cooldown ended"
            )

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
    """Scan files using local upload history as the duplicate source of truth."""
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
            # Do this during the startup/pre-upload scan, not only after a
            # fresh upload. It clears local copies that a prior run left
            # behind while retaining the iCloud Drive item itself.
            release_already_uploaded_copy(f)
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
    logger.info(f"BULK UPLOAD PHASE - {len(existing)} video(s) to process")
    logger.info("Videos are processed one at a time (download -> upload -> clean up).")
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

    cleanup_only = "--cleanup-zombies" in sys.argv
    dedupe_delete_only = "--dedupe-icloud-delete" in sys.argv
    hash_backfill_only = "--backfill-upload-hashes" in sys.argv
    if sum((cleanup_only, dedupe_delete_only, hash_backfill_only)) > 1:
        logger.error("Use only one maintenance mode at a time.")
        sys.exit(2)
    stuck_after_hours = ZOMBIE_PROCESSING_AGE_HOURS
    for argument in sys.argv[1:]:
        if argument.startswith("--cleanup-stuck-after-hours="):
            try:
                stuck_after_hours = max(1, int(argument.split("=", 1)[1]))
            except ValueError:
                logger.error("--cleanup-stuck-after-hours must be a positive integer.")
                sys.exit(2)

    logger.info("=" * 50)
    logger.info("iCloud -> YouTube Uploader Started")
    logger.info(f"  Watch folder: {ICLOUD_FOLDER}")
    logger.info(f"  Playlist ID:  {PLAYLIST_ID}")
    logger.info(f"  Log file:     {LOG_FILE}")
    logger.info(f"  History file: {HISTORY_FILE}")
    logger.info(f"  Video types:  {', '.join(VIDEO_EXTENSIONS)}")
    logger.info("  Policy: one video at a time; failed videos are offloaded back "
                "to iCloud, never deleted.")
    logger.info("=" * 50)

    if not cleanup_only and not dedupe_delete_only and not os.path.exists(ICLOUD_FOLDER):
        logger.error(f"Watch folder does not exist: {ICLOUD_FOLDER}")
        logger.error("Please create the folder or update ICLOUD_FOLDER in the script.")
        sys.exit(1)

    if dedupe_delete_only:
        logger.warning(
            "ICLOUD DUPLICATE DELETE MODE: byte-identical files will be "
            "permanently removed from iCloud; the oldest copy is retained."
        )
        delete_content_duplicates(Path(ICLOUD_FOLDER))
        return

    if hash_backfill_only:
        logger.warning(
            "UPLOAD-HISTORY HASH BACKFILL MODE: existing uploaded videos will "
            "be downloaded and hashed one at a time; no videos will upload."
        )
        backfill_uploaded_video_hashes(Path(ICLOUD_FOLDER))
        return

    # Step 1: Authenticate
    logger.info("Authenticating with YouTube API...")
    youtube = get_authenticated_service()
    if not youtube:
        logger.error("YouTube authentication failed. Exiting.")
        sys.exit(1)
    logger.info("YouTube API authenticated successfully.\n")

    if cleanup_only:
        logger.warning(
            f"ZOMBIE CLEANUP MODE: deleting failed/rejected videos and videos "
            f"stuck processing for {stuck_after_hours}+ hour(s)."
        )
        remove_zombie_videos_from_playlist(youtube, stuck_after_hours)
        return

    # The playlist is the source of truth for title numbers, including videos
    # uploaded before this script or by a previous script run.
    sync_sequence_from_playlist(youtube)
    reconcile_online_playlist(youtube, "startup")

    # Step 2: Upload all existing videos first
    youtube = bulk_upload_existing(youtube)

    # Step 3: Now start watching for new videos
    logger.info("\nSwitching to WATCH MODE - monitoring for new videos...")
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
            reconcile_online_playlist(youtube, "scheduled wake-up")

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
    try:
        main()
    except KeyboardInterrupt:
        # Ctrl+C can arrive during bulk hydration/upload, before main() reaches
        # its watch-mode try/except. Treat it as the normal requested shutdown
        # rather than allowing Python to print a traceback.
        logger.info("Shutdown complete.")
