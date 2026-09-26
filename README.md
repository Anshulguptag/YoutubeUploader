# iCloud to YouTube Uploader

A Windows Python uploader that watches an iCloud Drive folder, uploads video files to YouTube, adds them to a playlist, and avoids duplicate uploads.

## Setup

1. Create and activate a Python virtual environment (Python 3.8 or newer).
2. Install the dependencies:

   ```powershell
   pip install -r requirements.txt
   ```

3. In Google Cloud Console, create two OAuth Desktop client credentials with the YouTube Data API enabled. Place their downloaded files beside the script as:

   - `client_secret.json`
   - `client_secret_2.json`

4. Edit `ICLOUD_FOLDER` and `PLAYLIST_ID` in `icloud_to_youtube_uploader.py`.
5. Run the uploader:

   ```powershell
   python .\icloud_to_youtube_uploader.py
   ```

The first run opens a browser for OAuth authorization and creates local tokens. OAuth credentials, tokens, upload history, title state, retry state, and logs are intentionally excluded from Git.

## Behavior

- Processes **one video at a time**: it downloads the current video from iCloud, uploads it, and only then picks up the next file. Two videos are never downloaded or uploaded at the same time.
- Uploads existing videos first, then watches the configured iCloud folder.
- **Nothing is ever deleted from iCloud.** Once a video has been processed, its local copy is offloaded (dehydrated) exactly like Explorer's *Free up space* (`attrib +U -P`), so the video stays in iCloud while the disk space is freed. Opening or re-uploading it downloads it again.
- `uploaded_history.txt` records each successful upload as `SHA-256 hash : original filename : title`. The local history and hashes are the duplicate source of truth: folder scans do not query YouTube to decide whether a file was uploaded. A copied or renamed version of an uploaded video is skipped as well. To add hashes to older filename-only entries without delaying normal uploads, run `python .\icloud_to_youtube_uploader.py --backfill-upload-hashes`; it processes matching iCloud sources one at a time and offloads each afterward. Entries whose original source is no longer in the folder remain filename-only until their bytes are available.
- Videos that fail to download or upload are **offloaded, never deleted** — the failure is recorded in `failed_videos.txt`, the local copy is unpinned (the video stays in iCloud), and the uploader immediately moves on to the next video. Files listed there are skipped by later scans so a broken file is not downloaded and retried forever. Delete an entry (or the whole file) to retry that video.
- When YouTube's daily upload limit is reached, uploads pause and the current video is **kept locally** so it can be retried automatically after the cooldown.
- Uses resumable uploads with retries for temporary network errors.
- Each newly uploaded video's description records the YouTube channel and configured OAuth-account number that uploaded it.
- Each newly uploaded video's description also records the source file's SHA-256 hash, matching its `uploaded_history.txt` entry. This makes the hash recoverable from YouTube metadata for future uploads.
- Before each upload the video is **copied to a plain local staging folder** (`upload_staging/`, outside iCloud). This prevents the `[Errno 22]` mid-upload read failures iCloud placeholders can cause, which previously left broken videos stuck in "Processing" on YouTube forever. The staged copy is deleted as soon as the upload finishes, and if an upload does fail the script now automatically removes the half-finished video it left behind.
- Falls back to the second OAuth client when the first client reaches quota.
- Reads the playlist to continue `Date_YYYYMMDD_NNN` title sequencing.
- Detects how much of each video is really stored locally (`GetCompressedFileSize`), so cloud-only iCloud files are downloaded before upload instead of failing with read errors.
- Tuning knobs: `OFFLOAD_LOCAL_COPY`, `OFFLOAD_WAIT_TIMEOUT`, `SKIP_FAILED_VIDEOS`, `DOWNLOAD_WAIT_TIMEOUT`, `MAX_DOWNLOAD_ATTEMPTS`, `DOWNLOAD_RETRY_DELAY`.

## Large files and disk space

The YouTube API uploads bytes from the computer running this script; it cannot
give YouTube an iCloud Drive URL for a server-to-server transfer. In addition,
iCloud for Windows must hydrate the full source file on the volume where iCloud
Drive lives so resumable uploads can safely seek and retry.

Therefore, a 60 GB cloud-only iCloud file cannot be uploaded reliably from a
30 GB iCloud Drive volume. The script now checks this before starting a partial
download and reports the required space. Move iCloud Drive to an external drive
or another local volume with at least the file size plus 2 GiB free, let the
file fully download there, and then run the uploader. The optional staging copy
also needs another file-sized amount of free space; if it is unavailable, the
script uploads from the fully hydrated iCloud file directly.

## Permanently removing exact duplicate files

Normal uploader runs never delete iCloud files. To scan the direct contents of
`ICLOUD_FOLDER`, retain the oldest file in every byte-identical group, and
permanently delete the later copies from iCloud, run:

```powershell
python .\icloud_to_youtube_uploader.py --dedupe-icloud-delete
```

It compares SHA-256 content hashes after first grouping files by size; matching
names or file sizes alone are not deleted. Hashing a cloud-only iCloud file
requires it to be hydrated, so a file that does not fit in the available local
space is safely skipped. Deleted files are removed from iCloud Drive as well.

## Organizing iCloud photos and videos

To preview direct files that would be organized into `Images` and `Videos`
inside the configured iCloud folder, run:

```powershell
python .\organize_icloud_media.py
```

After reviewing the preview, perform the moves with:

```powershell
python .\organize_icloud_media.py --apply
```

The organizer never deletes files or overwrites an existing destination. A
same-stem image and `.MOV` pair is recognized as an Apple Live Photo, and both
files are moved to `Images`. The uploader watches only the resulting `Videos`
folder, so it does not upload anything from `Images`, including Live Photo
`.MOV` companions.

## Cleaning crashed or zombie playlist videos

Use the standalone cleanup script to inspect the target playlist. It reports
only videos YouTube identifies as failed, rejected, deleted, terminated, or
still processing for 48+ hours:

```powershell
python .\cleanup_youtube_playlist.py
```

After reviewing the report, remove only those entries from the playlist:

```powershell
python .\cleanup_youtube_playlist.py --apply
```

To also permanently delete the underlying videos when the authenticated account
owns them, add `--delete-videos`. A different playlist can be selected with
`--playlist-id`, and the processing threshold can be changed with
`--stuck-after-hours 72`.

The script checks all configured uploader OAuth accounts (`token.json`,
`token_2.json`, and `token_3.json`). Entries none of them can retrieve are
reported as **unavailable**. After reviewing those results, remove them with:

```powershell
python .\cleanup_youtube_playlist.py --apply --remove-unavailable
```

## Security

Never commit `client_secret*.json` or `token*.json`. They grant access to your Google/YouTube account.
