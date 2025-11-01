import os
import subprocess
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import json

# ---------- CONFIG ----------
URLS_FILE = "urls.txt"  # Your livestream URLs
DOWNLOAD_FOLDER = "downloads"  # Folder where ytarchive will save recordings

# ---------- GOOGLE DRIVE ----------
TOKEN_FILE = "token.json"

# Read Google token
creds = None
if os.path.exists(TOKEN_FILE):
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, ["https://www.googleapis.com/auth/drive.file"])
service = build("drive", "v3", credentials=creds)

def upload_to_drive(file_path):
    file_metadata = {'name': os.path.basename(file_path)}
    media = MediaFileUpload(file_path, resumable=True)
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"Uploaded {file_path} to Google Drive.")

# ---------- MAIN ----------
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

with open(URLS_FILE, "r") as f:
    urls = [line.strip() for line in f if line.strip()]

for url in urls:
    print(f"Recording livestream: {url}")
    # ytarchive command
    result = subprocess.run([
        "ytarchive",
        "record",
        url,
        "--output", os.path.join(DOWNLOAD_FOLDER, "%(title)s.%(ext)s")
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error recording {url}: {result.stderr}")
        continue

# Upload all files in DOWNLOAD_FOLDER to Google Drive
for filename in os.listdir(DOWNLOAD_FOLDER):
    file_path = os.path.join(DOWNLOAD_FOLDER, filename)
    if os.path.isfile(file_path):
        upload_to_drive(file_path)
