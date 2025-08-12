import os
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from labelbox import Client
from google.cloud import storage

load_dotenv()

# --- Configuration ---

# 1. Local Directory of your MP4 files
LOCAL_SOURCE_DIRECTORY = "labelbox_sidewalk_testbed_data"

# 2. Google Cloud Storage Destination Details
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID")
DESTINATION_BUCKET_NAME = os.getenv("DESTINATION_BUCKET_NAME")

# 3. Labelbox Details
LABELBOX_API_KEY = os.getenv("LABELBOX_API_KEY")
LABELBOX_DATASET_ID = os.getenv("LABELBOX_DATASET_ID")  # Set this in your .env or replace with actual id

# 4. CSV file listing required videos (update as needed)
CSV_PATH = "data/testbed_2025.csv"


def main():
    # --- Step 1: Initialize Clients ---
    try:
        storage_client = storage.Client(project=GCS_PROJECT_ID)
        lb_client = Client(api_key=LABELBOX_API_KEY)
        print("✅ Successfully initialized Google Cloud Storage and Labelbox clients.")
    except Exception as e:
        print(f"❌ Error initializing clients: {e}")
        return

    # --- Step 2: Get the Labelbox Dataset ---
    try:
        dataset = lb_client.get_dataset(LABELBOX_DATASET_ID)
    except Exception as e:
        print(f"❌ Error getting Labelbox dataset: {e}")
        return

    # --- Step 3: Read CSV and prepare video list ---
    df = pd.read_csv(CSV_PATH)
    # Convert .MOV to .mp4 for video_name
    def mov_to_mp4(name):
        if name.lower().endswith('.mov'):
            return name[:-4] + '.mp4'
        return name
    video_names = df['video_name'].apply(mov_to_mp4).tolist()

    # --- Step 4: Upload videos to GCP bucket and prepare for Labelbox ---
    try:
        destination_bucket = storage_client.get_bucket(DESTINATION_BUCKET_NAME)
    except Exception as e:
        print(f"❌ Error connecting to GCP bucket: {e}")
        return


    # Set global key prefix for Labelbox
    GLOBAL_KEY_PREFIX = "testbed2025"  # Change this as needed

    data_rows_to_create = []
    base_gcs_url = "https://storage.googleapis.com"

    for video_name in tqdm(video_names):
        local_path = os.path.join(LOCAL_SOURCE_DIRECTORY, video_name)
        if not os.path.exists(local_path):
            print(f"❌ File not found: {local_path}")
            continue
        blob = destination_bucket.blob(video_name)
        print(f"Uploading {video_name} to GCS bucket...")
        blob.upload_from_filename(local_path)
        print("Upload complete.")
        gcs_uri = f"{base_gcs_url}/{DESTINATION_BUCKET_NAME}/{video_name}"
        # Add prefix to global_key
        global_key = f"{GLOBAL_KEY_PREFIX}_{os.path.splitext(video_name)[0]}"
        data_rows_to_create.append({
            "row_data": gcs_uri,
            "global_key": global_key
        })

    # --- Step 5: Create Data Rows in Labelbox ---
    if data_rows_to_create:
        print(f"\n✅ Creating {len(data_rows_to_create)} data rows in Labelbox...")
        task = dataset.create_data_rows(data_rows_to_create)
        task.wait_till_done()
        if task.errors:
            print("\n❌ Errors encountered during data row creation:")
            print(task.errors)
        else:
            print("\n✅ Successfully created all data rows in Labelbox!")

if __name__ == "__main__":
    main()
