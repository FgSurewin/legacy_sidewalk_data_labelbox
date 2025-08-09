# Legacy Sidewalk Data Labelbox Toolkit

This repository provides tools for managing, converting, uploading, and downloading sidewalk video datasets for use with Google Cloud Storage (GCP) and Labelbox. It supports both continuous and static video datasets, and includes utilities for batch processing, metadata management, and data transfer.

## Requirements

- Python 3.7+
- Install dependencies:

  ```bash
  pip install -r requirements.txt
  ```

  (You may need: `pandas`, `tqdm`, `requests`, `python-dotenv`, `google-cloud-storage`, `labelbox`, etc.)

- Set up a `.env` file in the project root with the following variables:
  ```env
  GCS_PROJECT_ID=your-gcp-project-id
  DESTINATION_BUCKET_NAME=your-gcs-bucket-name
  LABELBOX_API_KEY=your-labelbox-api-key
  LABELBOX_DATASET_ID=your-labelbox-dataset-id
  ```

## Scripts Overview

### 1. `convert_mp4_labelbox.py`

Convert all `.MOV` videos in a local directory to `.mp4`, upload them to a GCP bucket, and register them in a Labelbox dataset.

**Usage:**

```bash
python convert_mp4_labelbox.py
```

_Requires: Google credentials, Labelbox API key, and correct `.env` setup._

### 2. `upload_data.py`

Upload existing `.mp4` videos (no conversion) from `labelbox_sidewalk_legacy_data` to a GCP bucket and register them in a Labelbox dataset. Uses a CSV (e.g., `static_videos.csv`) to determine which videos to upload.

**Usage:**

```bash
python upload_data.py
```

_Set the `GLOBAL_KEY_PREFIX` variable in the script to control the prefix for Labelbox global keys (default: `static`)._

### 3. `upload_labelbox_only.py`

Register videos in a Labelbox dataset using public GCS links (no upload to GCP). Uses a CSV to determine which videos to register. Useful if videos are already in the bucket.

**Usage:**

```bash
python upload_labelbox_only.py
```

### 4. `download_gcp_videos.py`

Download all videos from a GCP bucket (specified by `.env`) to a local folder (`labelbox_sidewalk_legacy_data`).

**Usage:**

```bash
python download_gcp_videos.py
```

### 5. `download_from_csv.py`

Download videos listed in a CSV file (with a `download_link` column) to a local output folder, organizing by `collector_name`.

**Usage:**

```bash
python download_from_csv.py --csv static_videos_with_link.csv --output ./downloaded_videos
```

_You can use any CSV with columns: `download_link`, `video_name`, `collector_name`._

## Example Workflow

1. **Convert and upload MOV videos:**

   - Place all `.MOV` files in the source directory.
   - Run `convert_mp4_labelbox.py` to convert and upload.

2. **Upload static/continuous videos using CSV:**

   - Prepare a CSV (e.g., `static_videos.csv`) listing required videos.
   - Run `upload_data.py` to upload and register videos.

3. **Register already-uploaded videos in Labelbox:**

   - Prepare a CSV with video names and construct public GCS links.
   - Run `upload_labelbox_only.py` to register in Labelbox.

4. **Download videos from GCP bucket:**

   - Run `download_gcp_videos.py` to fetch all videos from the bucket.

5. **Download videos using CSV:**
   - Run `download_from_csv.py` with the desired CSV and output folder.

## Notes

- Ensure your GCP and Labelbox credentials are valid and have the necessary permissions.
- For large datasets, scripts use `tqdm` for progress bars and will skip already existing files when downloading.
- The codebase is modular; you can adapt scripts for your own data management needs.

## License

MIT License

# legacy_sidewalk_data_labelbox
