import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from ytarchive import YtArchive
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# ==========================
# CONFIGURATION
# ==========================
DOWNLOAD_DIR = "downloads"
URLS_FILE = "urls.txt"
MAX_CONCURRENT = 4
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds
LOG_FILE = "yt_archive.log"

# ==========================
# SETUP LOGGING
# ==========================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger().addHandler(logging.StreamHandler())

# ==========================
# GOOGLE DRIVE AUTH
# ==========================
gauth = GoogleAuth()

# First time setup: run this locally to authenticate and save credentials.json
# Then push credentials.json to repo or GitHub Secrets if using Actions
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

# ==========================
# HELPER FUNCTIONS
# ==========================
def record_and_upload(url: str):
    """Record livestream with ytarchive and upload to Google Drive."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"Starting recording for: {url} (Attempt {attempt})")

            # Record livestream
            archive = YtArchive(url, output_dir=DOWNLOAD_DIR)
            archive.record()  # blocks until livestream ends
            filename = archive.latest_file

            if not filename or not os.path.exists(filename):
                raise FileNotFoundError("Recorded file not found")

            logging.info(f"Finished recording: {filename}")

            # Upload to Google Drive
            file_drive = drive.CreateFile({'title': os.path.basename(filename)})
            file_drive.SetContentFile(filename)
            file_drive.Upload()
            logging.info(f"Uploaded {filename} to Google Drive")

            # Delete local copy
            os.remove(filename)
            logging.info(f"Deleted local file: {filename}")

            break  # success, exit retry loop

        except Exception as e:
            logging.error(f"Error with URL {url}: {e}")
            if attempt < MAX_RETRIES:
                logging.info(f"Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                logging.error(f"Failed to process {url} after {MAX_RETRIES} attempts")


# ==========================
# MAIN
# ==========================
def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Load URLs
    if not os.path.exists(URLS_FILE):
        logging.error(f"{URLS_FILE} not found! Exiting.")
        return

    with open(URLS_FILE, "r") as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        logging.info("No URLs found in urls.txt. Exiting.")
        return

    logging.info(f"Starting processing {len(urls)} URLs...")

    # Run concurrent recordings
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        executor.map(record_and_upload, urls)

    logging.info("All tasks completed.")


if __name__ == "__main__":
    main()
