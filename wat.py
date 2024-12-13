from google.cloud import bigquery
import os

# Set your GCP project and dataset details
PROJECT_ID = "iot-traffic-444216"
DATASET_ID = "traffic_data"

# CSV files to upload
FILES_TO_UPLOAD = {
    "traffic_flow": "/home/abaltayeva99/traffic/clustered_traffic_with_names.csv",
    "air_quality": "/home/abaltayeva99/traffic/air_quality_by_cluster.csv",
    "weather": "/home/abaltayeva99/traffic//weather_london.csv ",
}

def upload_csv_to_bq(table_name, file_path):
    """
    Uploads a CSV file to BigQuery, creating a new table.
    
    :param table_name: Name of the table to create in BigQuery.
    :param file_path: Path to the CSV file to upload.
    """
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)

    # Construct table ID
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # Define table schema (can be auto-detected or manually defined here)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row in CSV
        autodetect=True,  # Enable schema autodetection
    )

    # Open the CSV file and load it into BigQuery
    with open(file_path, "rb") as file:
        load_job = client.load_table_from_file(file, table_id, job_config=job_config)
    
    # Wait for the load job to complete
    load_job.result()
    print(f"Table {table_name} created and data uploaded successfully.")

def main():
    # Iterate through the CSV files and upload them as new tables
    for table_name, file_path in FILES_TO_UPLOAD.items():
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} does not exist. Skipping {table_name}.")
            continue
        
        try:
            upload_csv_to_bq(table_name, file_path)
        except Exception as e:
            print(f"Failed to upload {file_path} to table {table_name}: {e}")

if _name_ == "_main_":
    main()