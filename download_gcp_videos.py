import os
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# 1. GCS Bucket Details
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID")
DESTINATION_BUCKET_NAME = os.getenv("DESTINATION_BUCKET_NAME")

# 2. Local download directory
LOCAL_DOWNLOAD_DIR = "labelbox_sidewalk_legacy_data"


def download_all_videos_from_bucket():
    # Create local directory if it doesn't exist
    if not os.path.exists(LOCAL_DOWNLOAD_DIR):
        os.makedirs(LOCAL_DOWNLOAD_DIR)

    # Initialize GCP Storage client
    try:
        storage_client = storage.Client(project=GCS_PROJECT_ID)
        bucket = storage_client.get_bucket(DESTINATION_BUCKET_NAME)
        print(f"✅ Connected to bucket: {DESTINATION_BUCKET_NAME}")
    except Exception as e:
        print(f"❌ Error connecting to GCP bucket: {e}")
        return

    # List all blobs (files) in the bucket
    blobs = bucket.list_blobs()
    video_files = [blob for blob in blobs if blob.name.lower().endswith(('.mp4', '.mov'))]
    print(f"Found {len(video_files)} video files in the bucket.")

    for blob in video_files:
        local_path = os.path.join(LOCAL_DOWNLOAD_DIR, os.path.basename(blob.name))
        print(f"Downloading {blob.name} to {local_path} ...")
        try:
            blob.download_to_filename(local_path)
            print("Download complete.")
        except Exception as e:
            print(f"❌ Error downloading {blob.name}: {e}")

if __name__ == "__main__":
    download_all_videos_from_bucket()
