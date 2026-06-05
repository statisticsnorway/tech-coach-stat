from google.cloud import storage
import os
import zipfile
from pathlib import Path

# Change these variables
COMPOSER_BUCKET = "ssb-tip-tutorials-automation-composer"
SOURCE_FILE_PATH= "zip_simple_virtualenv_2.zip"
DESTINATION_PATH = "dags/zip_simple_virtualenv_2.zip"
WORKFLOW_FILE = "simple_virtualenv.py"

def zip_current_directory():
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    print(script_path)
    try:
        with zipfile.ZipFile(SOURCE_FILE_PATH, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(script_dir):
                for file in files:
                    if "upload" in file: 
                        continue
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, script_dir)
                    print(arcname)
                    zipf.write(file_path, arcname)
    except Exception as e:
        print(f"Error creating zip: {e}")

def upload_file(bucket_name, source_file_path, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name}.")
if __name__ == "__main__":
    zip_current_directory()
    upload_file(COMPOSER_BUCKET, SOURCE_FILE_PATH, DESTINATION_PATH)