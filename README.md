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
- Videos that fail to download or upload are **offloaded, never deleted** — the failure is recorded in `failed_videos.txt`, the local copy is unpinned (the video stays in iCloud), and the uploader immediately moves on to the next video. Files listed there are skipped by later scans so a broken file is not downloaded and retried forever. Delete an entry (or the whole file) to retry that video.
- When YouTube's daily upload limit is reached, uploads pause and the current video is **kept locally** so it can be retried automatically after the cooldown.
- Uses resumable uploads with retries for temporary network errors.
- Before each upload the video is **copied to a plain local staging folder** (`upload_staging/`, outside iCloud). This prevents the `[Errno 22]` mid-upload read failures iCloud placeholders can cause, which previously left broken videos stuck in "Processing" on YouTube forever. The staged copy is deleted as soon as the upload finishes, and if an upload does fail the script now automatically removes the half-finished video it left behind.
- Falls back to the second OAuth client when the first client reaches quota.
- Reads the playlist to continue `Date_YYYYMMDD_NNN` title sequencing.
- Detects how much of each video is really stored locally (`GetCompressedFileSize`), so cloud-only iCloud files are downloaded before upload instead of failing with read errors.
- Tuning knobs: `OFFLOAD_LOCAL_COPY`, `OFFLOAD_WAIT_TIMEOUT`, `SKIP_FAILED_VIDEOS`, `DOWNLOAD_WAIT_TIMEOUT`, `MAX_DOWNLOAD_ATTEMPTS`, `DOWNLOAD_RETRY_DELAY`.

## Security

Never commit `client_secret*.json` or `token*.json`. They grant access to your Google/YouTube account.
