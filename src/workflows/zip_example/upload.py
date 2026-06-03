from google.cloud import storage
import os
import zipfile
import sys

# Change these variables
COMPOSER_BUCKET = "ssb-tip-tutorials-automation-composer"
SOURCE_FILE_PATH= "zip-workflow.zip"
DESTINATION_PATH = "dags/hallo_workflow.py"

def zip_current_directory():
    script_path = os.path.abspath(__file__)
    script_name = os.path.basename(script_path)
    
    base_dir = os.path.dirname(script_path)
    
    zip_filename = os.path.join("zip-workflow.zip")
    
    try:
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(base_dir):
                for file in files:
                    if file == script_name or file == os.path.basename(zip_filename):
                        continue
                    
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, base_dir)
                    zipf.write(file_path, arcname)
    except Exception as e:
        print(f"Error creating zip: {e}")
        sys.exit(1)

def upload_file(bucket_name, source_file_path, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name}.")
if __name__ == "__main__":
    zip_current_directory()
    # upload_file(COMPOSER_BUCKET, SOURCE_FILE_PATH, DESTINATION_PATH)