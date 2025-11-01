import os
import threading
import time
import logging
from ytarchive.ytarchive import download_live
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ---------------- CONFIG -----------------
URLS_FILE = "urls.txt"
DOWNLOAD_DIR = "downloads"
LOG_FILE = "yt_archive.log"
MAX_RETRIES = 3
DRIVE_FOLDER_ID = None  # optional: set your Google Drive folder ID

# ----------------------------------------

# Setup logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Authenticate Google Drive
def google_drive_service():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json")
    else:
        logging.error("Google Drive credentials not found. Run OAuth flow to generate token.json.")
        raise FileNotFoundError("token.json not found")
    service = build('drive', 'v3', credentials=creds)
    return service

# Upload to Google Drive
def upload_to_drive(file_path, service):
    file_metadata = {'name': os.path.basename(file_path)}
    if DRIVE_FOLDER_ID:
        file_metadata['parents'] = [DRIVE_FOLDER_ID]
    media = MediaFileUpload(file_path, resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    logging.info(f"Uploaded {file_path} to Google Drive (ID: {file['id']})")

# Record a single URL
def record_url(url):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            logging.info(f"Starting recording for {url} (Attempt {retries+1})")
            file_path = download_live(url, DOWNLOAD_DIR)
            logging.info(f"Finished recording: {file_path}")
            
            # Upload
            drive_service = google_drive_service()
            upload_to_drive(file_path, drive_service)
            
            # Remove local copy
            os.remove(file_path)
            logging.info(f"Deleted local file: {file_path}")
            break
        except Exception as e:
            logging.error(f"Error recording {url}: {e}")
            retries += 1
            time.sleep(60)  # wait a minute before retry

# Main loop
def main():
    with open(URLS_FILE, "r") as f:
        urls = [line.strip() for line in f if line.strip()]
    
    threads = []
    for url in urls:
        t = threading.Thread(target=record_url, args=(url,))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
