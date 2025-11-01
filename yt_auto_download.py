import subprocess
import os
import time
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# =============== CONFIGURATION =====================
# List your livestream URLs here
STREAM_URLS = [
    "https://www.youtube.com/watch?v=TgYw7Rqce-A",
    "https://www.youtube.com/watch?v=dEhQ6eoWtR4"
]

# Folder name in Google Drive where files will be uploaded
GOOGLE_DRIVE_FOLDER = "YouTube Livestream Recordings"
# ====================================================


def setup_drive():
    """Authenticate and connect to Google Drive."""
    print("🔑 Authenticating with Google Drive...")
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # opens a browser for first-time authentication
    return GoogleDrive(gauth)


def ensure_drive_folder(drive, folder_name):
    """Create a folder in Drive if it doesn't exist."""
    file_list = drive.ListFile({'q': "mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList()
    for folder in file_list:
        if folder['title'] == folder_name:
            print(f"📁 Found existing folder: {folder_name}")
            return folder['id']
    folder_metadata = {'title': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
    folder = drive.CreateFile(folder_metadata)
    folder.Upload()
    print(f"✅ Created folder: {folder_name}")
    return folder['id']


def download_stream(url):
    """Download livestream with ytarchive."""
    video_id = url.split("v=")[-1]
    output_name = f"{video_id}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.mp4"
    print(f"🎥 Downloading {url} ...")
    cmd = f"ytarchive {url} best -o {output_name} --wait"
    subprocess.run(cmd, shell=True)
    return output_name


def upload_to_drive(drive, folder_id, filename):
    """Upload file to Google Drive folder."""
    print(f"📤 Uploading {filename} to Google Drive...")
    f = drive.CreateFile({'title': filename, 'parents': [{'id': folder_id}]})
    f.SetContentFile(filename)
    f.Upload()
    print(f"✅ Uploaded {filename} successfully.")
    os.remove(filename)
    print(f"🗑️ Deleted local file {filename} to save space.")


def main():
    drive = setup_drive()
    folder_id = ensure_drive_folder(drive, GOOGLE_DRIVE_FOLDER)

    for url in STREAM_URLS:
        try:
            filename = download_stream(url)
            upload_to_drive(drive, folder_id, filename)
        except Exception as e:
            print(f"❌ Error processing {url}: {e}")

    print("🎉 All livestreams processed!")


if __name__ == "__main__":
    main()
