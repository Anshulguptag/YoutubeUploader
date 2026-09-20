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

- Uploads existing videos first, then watches the configured iCloud folder.
- Uses resumable uploads with retries for temporary network errors.
- Falls back to the second OAuth client when the first client reaches quota.
- Reads the playlist to continue `Date_YYYYMMDD_NNN` title sequencing.
- Hydrates cloud-only iCloud files before upload to avoid read/seek errors.

## Security

Never commit `client_secret*.json` or `token*.json`. They grant access to your Google/YouTube account.
