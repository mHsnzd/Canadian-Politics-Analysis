import s3fs
import os
import sys

# Add parent directory to path for importing constants
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def download_s3_folder(bucket_name, s3_folder, local_dir):
    """
    Downloads all files from a specific folder in an S3 bucket to a local directory.

    Args:
        bucket_name (str): The name of the S3 bucket.
        s3_folder (str): The folder path in the S3 bucket (e.g., 'folder/subfolder/').
        local_dir (str): The local directory where files will be downloaded.
    """
    # Initialize S3 client
    s3 = s3fs.S3FileSystem(
        anon=False,
        key=AWS_ACCESS_KEY_ID,
        secret=AWS_ACCESS_KEY
    )

    # Ensure local directory exists
    os.makedirs(local_dir, exist_ok=True)

    # List all files in the S3 folder
    s3_path = f"{bucket_name}/{s3_folder}/"
    files = s3.ls(s3_path)

    for file in files:
        # Exclude directories
        if file.endswith('/'):
            continue
        
        # Get file name
        file_name = os.path.basename(file)

        # Download file to the local directory
        local_file_path = os.path.join(local_dir, file_name)
        if os.path.exists(local_file_path):
            continue
        print(f"Downloading {file} to {local_file_path}...")
        s3.get(file, local_file_path)

    print("Download complete.")

# Example usage
bucket_name = 'cmpt732'
s3_folder = 'raw'  # e.g., 'mydata/reddit/'
local_dir = '../data/output'  # e.g., './downloads/'

download_s3_folder(bucket_name, s3_folder, local_dir)
