from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import os

# Load credentials
creds = Credentials.from_authorized_user_file("token.json", ["https://www.googleapis.com/auth/drive.file"])

# Build Drive service
service = build('drive', 'v3', credentials=creds)

# ID of the folder in Google Drive where files will be uploaded
FOLDER_ID = "1lVh1B2fSODUiJwyRb9BNpEJGqFyJaccD"

# Upload all files in recordings/
for filename in os.listdir("recordings"):
    filepath = os.path.join("recordings", filename)
    if os.path.isfile(filepath):
        file_metadata = {
            'name': filename,
            'parents': [FOLDER_ID]
        }
        media = MediaFileUpload(filepath)
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Uploaded {filename}")
