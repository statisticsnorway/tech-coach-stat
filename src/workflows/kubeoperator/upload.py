from google.cloud import storage
# Change these variables
COMPOSER_BUCKET = "ssb-tip-tutorials-automation-composer"
SOURCE_FILE_PATH= "src/workflows/kubeoperator/test_kubeop.py"
DESTINATION_PATH = "dags/test_kubeop.py"
def upload_file(bucket_name, source_file_path, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name}.")
if __name__ == "__main__":
    upload_file(COMPOSER_BUCKET, SOURCE_FILE_PATH, DESTINATION_PATH)