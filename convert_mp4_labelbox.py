import os
import shutil
import datetime
import glob
from tqdm import tqdm
from moviepy import VideoFileClip
from dotenv import load_dotenv
from labelbox import Client, DataRow
from google.cloud import storage

load_dotenv()

# --- Configuration ---

# 1. Local Directory of your MOV files
LOCAL_SOURCE_DIRECTORY = "./10_3_2023_all_data"

# 2. Google Cloud Storage Destination Details
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID")
DESTINATION_BUCKET_NAME = os.getenv("DESTINATION_BUCKET_NAME")
# # Optional: Specify a destination folder within the bucket
# DESTINATION_FOLDER_PREFIX = "videos/converted/"

# 3. Labelbox Details
LABELBOX_API_KEY = os.getenv("LABELBOX_API_KEY")
LABELBOX_DATASET_NAME = "sidewalk-legacy-data"

# 4. Local temporary directory for MP4 outputs before upload
TEMP_OUTPUT_DIR = "temp_mp4_outputs"


def convert_mov_to_mp4(mov_path, mp4_path):
    """Converts a MOV video file to MP4 format using moviepy."""
    try:
        print(f"Starting conversion: {os.path.basename(mov_path)} -> {os.path.basename(mp4_path)}")
        # Use a 'with' statement to ensure the clip is closed properly
        with VideoFileClip(mov_path) as video_clip:
            video_clip.write_videofile(mp4_path, codec="libx264", logger=None)
        print("Conversion successful.")
        return True
    except Exception as e:
        print(f"Error converting {mov_path} to MP4: {e}")
        return False
    
def main():
    # --- Step 1: Initialize Clients ---
    try:
        # client = storage.Client()
        storage_client = storage.Client(project=GCS_PROJECT_ID)
        lb_client = Client(api_key=LABELBOX_API_KEY)
        print("✅ Successfully initialized Google Cloud Storage and Labelbox clients.")
    except Exception as e:
        print(f"❌ Error initializing clients: {e}")
        return

    # --- Step 2: Get the Labelbox Dataset ---
    # You can get this id from lablebox instruction page.
    dataset = lb_client.get_dataset("cmcz04t1c00cy0763ft3ov0bv")

    # --- Step 3: Set up local temporary output directory ---
    if not os.path.exists(TEMP_OUTPUT_DIR):
        os.makedirs(TEMP_OUTPUT_DIR)

    # --- Step 4: Process Videos from Local Directory ---
    try:
        # Find all .mov files in the source directory
        local_mov_files = glob.glob(os.path.join(LOCAL_SOURCE_DIRECTORY, "**", '*.MOV'), recursive=True)
        if not local_mov_files:
            print(f"❌ No .mov files found in '{LOCAL_SOURCE_DIRECTORY}'. Please check the path.")
            return

        destination_bucket = storage_client.get_bucket(DESTINATION_BUCKET_NAME)
        data_rows_to_create = []

        print(f"\nFound {len(local_mov_files)} MOV files. Starting processing workflow...")
        for local_mov_path in tqdm(local_mov_files):
            file_name_with_ext = os.path.basename(local_mov_path)
            unique_global_key = os.path.splitext(file_name_with_ext)[0]

            local_mp4_path = os.path.join(TEMP_OUTPUT_DIR, f"{unique_global_key}.mp4")

            # Convert MOV to MP4
            if convert_mov_to_mp4(local_mov_path, local_mp4_path):
                # Upload MP4 to destination GCS bucket
                mp4_blob_name = f"{unique_global_key}.mp4"
                mp4_blob = destination_bucket.blob(mp4_blob_name)

                print(f"Uploading {os.path.basename(local_mp4_path)} to gs://{mp4_blob_name}...")
                mp4_blob.upload_from_filename(local_mp4_path)
                print("Upload complete.")

                # # Generate a signed URL for secure, temporary access
                # signed_url = mp4_blob.generate_signed_url(
                #     version="v4",
                #     expiration=datetime.timedelta(days=7), # URL is valid for 7 days
                #     method="GET"
                # )
                # https://storage.googleapis.com/labelbox-sidewalk-legacy-data/6_2023-06-30_11-15-18-5960_None.mp4
                base_gcs_ulr = "https://storage.googleapis.com"
                gcs_uri = f"{base_gcs_ulr}/{DESTINATION_BUCKET_NAME}/{mp4_blob_name}"
                print(f"The file gcs_url is {gcs_uri}")
                # Prepare data row for Labelbox
                data_rows_to_create.append({
                    "row_data": gcs_uri,
                    "global_key": unique_global_key
                })
            # Clean up the local MP4 file after upload
            if os.path.exists(local_mp4_path):
                os.remove(local_mp4_path)

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

    except Exception as e:
        print(f"\n❌ An error occurred during the main processing loop: {e}")
    # finally:
    #     # --- Step 6: Clean up the temporary directory ---
    #     if os.path.exists(TEMP_OUTPUT_DIR):
    #         shutil.rmtree(TEMP_OUTPUT_DIR)
    #         print(f"\n✅ Cleaned up temporary output directory: {TEMP_OUTPUT_DIR}")



if __name__ == "__main__":
    main()
