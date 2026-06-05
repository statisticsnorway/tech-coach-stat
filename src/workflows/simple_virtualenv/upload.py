from google.cloud import storage
import os
import zipfile
from pathlib import Path

# Change these variables
COMPOSER_BUCKET = "ssb-tip-tutorials-automation-composer"
SOURCE_FILE_PATH= "zip_simple_virtualenv.zip"
DESTINATION_PATH = "dags/zip_simple_virtualenv.zip"
WORKFLOW_FILE = "simple_virtualenv.py"

def zip_current_directory():
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)
    root_dir = Path(script_dir).parents[2] # move to project root
    print(root_dir)
    try:
        with zipfile.ZipFile(SOURCE_FILE_PATH, 'w', zipfile.ZIP_DEFLATED) as zipf:
            workflow_path = os.path.join(script_dir, WORKFLOW_FILE)
            zipf.write(workflow_path, WORKFLOW_FILE)
            zipf.write(os.path.join(script_dir, "requirements.txt"), "requirements.txt")
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