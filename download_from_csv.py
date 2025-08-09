import os
import argparse
import pandas as pd
import requests
from tqdm import tqdm


def download_file(url, dest_path):
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"❌ Failed to download {url}: {e}")
        return False

def main(csv_path, output_dir):
    df = pd.read_csv(csv_path)
    if 'download_link' not in df.columns or 'video_name' not in df.columns or 'collector_name' not in df.columns:
        print("❌ CSV must contain 'download_link', 'video_name', and 'collector_name' columns.")
        return

    os.makedirs(output_dir, exist_ok=True)

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Downloading videos"):
        collector = str(row['collector_name'])
        video_name = str(row['video_name'])
        url = row['download_link']
        collector_folder = os.path.join(output_dir, collector)
        os.makedirs(collector_folder, exist_ok=True)
        dest_path = os.path.join(collector_folder, video_name)
        if os.path.exists(dest_path):
            continue  # Skip if already downloaded
        download_file(url, dest_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download videos from CSV download_link column.")
    parser.add_argument('--csv', type=str, required=True, help='Path to the CSV file (with download_link, video_name, collector_name columns)')
    parser.add_argument('--output', type=str, required=True, help='Output folder to save downloaded videos')
    args = parser.parse_args()
    main(args.csv, args.output)
