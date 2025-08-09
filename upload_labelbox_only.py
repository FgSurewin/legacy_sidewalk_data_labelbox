import os
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from labelbox import Client

load_dotenv()

# --- Configuration ---

# 1. GCS Bucket public URL prefix
GCS_BUCKET_NAME = "labelbox-sidewalk-legacy-data-static"  # Update if needed
BASE_GCS_URL = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}"

# 2. Labelbox Details
LABELBOX_API_KEY = os.getenv("LABELBOX_API_KEY")
LABELBOX_DATASET_ID = os.getenv("LABELBOX_DATASET_ID")  # Set this in your .env or replace with actual id

# 3. CSV file listing required videos (update as needed)
CSV_PATH = "data/static_videos.csv"


def main():
    # --- Step 1: Initialize Labelbox Client ---
    try:
        lb_client = Client(api_key=LABELBOX_API_KEY)
        print("✅ Successfully initialized Labelbox client.")
    except Exception as e:
        print(f"❌ Error initializing Labelbox client: {e}")
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

    data_rows_to_create = []
    for video_name in tqdm(video_names):
        gcs_uri = f"{BASE_GCS_URL}/{video_name}"
        # global_key: add 'static_' prefix and remove extension
        global_key = f"static_{os.path.splitext(video_name)[0]}"
        data_rows_to_create.append({
            "row_data": gcs_uri,
            "global_key": global_key
        })

    # --- Step 4: Create Data Rows in Labelbox ---
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
