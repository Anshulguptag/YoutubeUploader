"""Review a YouTube playlist and remove confirmed crashed/zombie videos.

Default mode is a read-only report. Use --apply to remove matching entries from
the playlist, and add --delete-videos only when the corresponding owned videos
should also be permanently deleted from YouTube.
"""

import argparse
import datetime as dt
import logging
from pathlib import Path
from typing import Dict, List, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_PLAYLIST_ID = "PLKwxP3c9cudg"
CLIENT_SECRET_FILES = [
    SCRIPT_DIR / "client_secret.json",
    SCRIPT_DIR / "client_secret_2.json",
    SCRIPT_DIR / "client_secret_3.json",
]
TOKEN_FILES = [
    SCRIPT_DIR / "token.json",
    SCRIPT_DIR / "token_2.json",
    SCRIPT_DIR / "token_3.json",
]
SCOPES = ["https://www.googleapis.com/auth/youtube"]
FAILED_UPLOAD_STATUSES = {"failed", "rejected", "deleted"}
FAILED_PROCESSING_STATUSES = {"failed", "terminated"}

logger = logging.getLogger("playlist-cleanup")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
logger.addHandler(handler)


def authenticated_service(client_secret_file: Path, token_file: Path, allow_oauth: bool):
    """Load one uploader account, using OAuth only for the primary account."""
    credentials = None
    if token_file.exists():
        credentials = Credentials.from_authorized_user_file(str(token_file), SCOPES)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            if not allow_oauth:
                return None
            if not client_secret_file.exists():
                raise FileNotFoundError(f"Missing OAuth client secret: {client_secret_file}")
            flow = InstalledAppFlow.from_client_secrets_file(str(client_secret_file), SCOPES)
            credentials = flow.run_local_server(port=0)
        token_file.write_text(credentials.to_json(), encoding="utf-8")
    return build("youtube", "v3", credentials=credentials)


def authenticated_services() -> List[object]:
    """Load each configured uploader account that has an OAuth token."""
    services = []
    for index, (client_secret_file, token_file) in enumerate(
            zip(CLIENT_SECRET_FILES, TOKEN_FILES), start=1):
        service = authenticated_service(
            client_secret_file, token_file, allow_oauth=index == 1
        )
        if service:
            services.append(service)
            logger.info(f"Loaded uploader OAuth account {index}.")
        elif token_file.exists():
            logger.warning(f"Could not use uploader OAuth account {index}.")
    if not services:
        raise RuntimeError("No usable YouTube OAuth accounts were found.")
    return services


def list_playlist_items(youtube, playlist_id: str) -> List[dict]:
    """Return every entry in the target playlist."""
    items = []
    page_token = None
    while True:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails", playlistId=playlist_id,
            maxResults=50, pageToken=page_token,
        ).execute()
        items.extend(response.get("items", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            return items


def video_status(youtube_services: List[object], video_id: str) -> Optional[dict]:
    """Read a status with whichever configured account owns the video."""
    fallback = None
    for youtube in youtube_services:
        try:
            response = youtube.videos().list(
                part="status,processingDetails", id=video_id
            ).execute()
            video = next(iter(response.get("items", [])), None)
            if video:
                # Owner-only processing details identify the account that can
                # reliably classify a stalled or failed upload.
                if "processingDetails" in video:
                    return video
                fallback = fallback or video
        except HttpError as exc:
            if exc.resp.status != 403:
                raise
    return fallback


def parse_timestamp(value: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None


def zombie_reason(video: dict, playlist_item: dict, cutoff: dt.datetime) -> Optional[str]:
    """Return a removal reason only for a YouTube-confirmed broken video."""
    status = video.get("status", {})
    processing = video.get("processingDetails", {})
    upload_status = status.get("uploadStatus")
    processing_status = processing.get("processingStatus")

    if upload_status in FAILED_UPLOAD_STATUSES:
        return status.get("failureReason") or status.get("rejectionReason") or upload_status
    if processing_status in FAILED_PROCESSING_STATUSES:
        return processing.get("processingFailureReason") or processing_status
    if processing_status != "processing":
        return None

    published_at = playlist_item.get("snippet", {}).get("publishedAt", "")
    published = parse_timestamp(published_at)
    if published and published <= cutoff:
        return f"processing longer than {int((dt.datetime.now(dt.timezone.utc) - published).total_seconds() // 3600)} hours"
    return None


def find_zombies(youtube_services: List[object], playlist_id: str,
                 stuck_after_hours: int) -> List[dict]:
    """Find playlist entries which YouTube says failed or are stale processing."""
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=stuck_after_hours)
    candidates = []
    for playlist_item in list_playlist_items(youtube, playlist_id):
        video_id = playlist_item.get("contentDetails", {}).get("videoId")
        if not video_id:
            continue
        try:
            video = video_status(youtube_services, video_id)
        except HttpError as exc:
            logger.warning(f"Could not inspect {video_id}; leaving it unchanged: {exc}")
            continue
        if not video:
            candidates.append({
                "playlist_item_id": playlist_item["id"],
                "video_id": video_id,
                "title": playlist_item.get("snippet", {}).get("title", "(untitled)"),
                "reason": "unavailable to all configured uploader accounts",
                "unavailable": True,
            })
            continue
        reason = zombie_reason(video, playlist_item, cutoff)
        if reason:
            candidates.append({
                "playlist_item_id": playlist_item["id"],
                "video_id": video_id,
                "title": playlist_item.get("snippet", {}).get("title", "(untitled)"),
                "reason": reason,
                "unavailable": False,
            })
    return candidates


def apply_cleanup(youtube, candidates: List[dict], delete_videos: bool) -> None:
    """Remove selected entries; optionally delete the underlying owned videos."""
    for candidate in candidates:
        title = candidate["title"]
        video_id = candidate["video_id"]
        try:
            youtube.playlistItems().delete(id=candidate["playlist_item_id"]).execute()
            logger.warning(f"Removed from playlist: {title} ({video_id})")
        except HttpError as exc:
            logger.error(f"Could not remove playlist item for {title}: {exc}")
            continue
        if not delete_videos:
            continue
        try:
            youtube.videos().delete(id=video_id).execute()
            logger.warning(f"Deleted YouTube video: {title} ({video_id})")
        except HttpError as exc:
            logger.error(
                f"Playlist entry was removed, but {video_id} was not deleted "
                f"(often because this account does not own it): {exc}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--playlist-id", default=DEFAULT_PLAYLIST_ID)
    parser.add_argument("--stuck-after-hours", type=int, default=48)
    parser.add_argument("--apply", action="store_true", help="Remove listed entries from the playlist.")
    parser.add_argument(
        "--remove-unavailable", action="store_true",
        help="With --apply, also remove entries unavailable to every configured account.",
    )
    parser.add_argument(
        "--delete-videos", action="store_true",
        help="Also permanently delete owned YouTube videos; requires --apply.",
    )
    args = parser.parse_args()
    if args.stuck_after_hours < 1:
        parser.error("--stuck-after-hours must be at least 1")
    if args.delete_videos and not args.apply:
        parser.error("--delete-videos requires --apply")
    if args.remove_unavailable and not args.apply:
        parser.error("--remove-unavailable requires --apply")

    youtube_services = authenticated_services()
    youtube = youtube_services[0]
    candidates = find_zombies(youtube_services, args.playlist_id, args.stuck_after_hours)
    zombies = [candidate for candidate in candidates if not candidate["unavailable"]]
    unavailable = [candidate for candidate in candidates if candidate["unavailable"]]
    logger.info(
        f"Playlist review complete: {len(zombies)} crashed/zombie and "
        f"{len(unavailable)} unavailable video(s) found."
    )
    for candidate in candidates:
        logger.warning(
            f"CANDIDATE | {candidate['title']} | {candidate['video_id']} | "
            f"{candidate['reason']}"
        )
    if not args.apply:
        logger.info("Report only: no playlist entries or videos were deleted. Re-run with --apply to clean the playlist.")
        return
    to_remove = zombies + (unavailable if args.remove_unavailable else [])
    if unavailable and not args.remove_unavailable:
        logger.warning(
            "Unavailable entries were not removed. Re-run with --apply "
            "--remove-unavailable after reviewing the report to remove them."
        )
    apply_cleanup(youtube, to_remove, args.delete_videos)


if __name__ == "__main__":
    main()
