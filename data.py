import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# Set the path to your service account key (Optional in GCP environments)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/service_account_key.json"

API_KEY = "Wgfpa3qxA6DyaB_TX2_QrJ0idhJTq_eJ4M_4nIcK7sE"
BASE_URL = f"https://data.traffic.hereapi.com/v7/flow?in=circle:51.509865,-0.118092;r=11600&locationReferencing=shape&apiKey={API_KEY}"

def get_traffic_data():
    """
    Fetch traffic data from the HERE API.
    """
    try:
        response = requests.get(BASE_URL)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch traffic data: {e}")
        return None

def process_traffic_data(data):
    """
    Process traffic data into a DataFrame.
    """
    try:
        if not data or 'results' not in data:
            print("No valid data to process.")
            return pd.DataFrame()

        locations = []
        for result in data['results']:
            location = result.get('location', {})
            current_flow = result.get('currentFlow', {})
            locations.append({
                "Description": location.get('description', 'N/A'),
                "Latitude": location.get('geoCoordinate', {}).get('latitude', None),
                "Longitude": location.get('geoCoordinate', {}).get('longitude', None),
                "Speed": current_flow.get('speed', 0.0),
                "Free Flow": current_flow.get('freeFlow', 0.0),
                "Jam Factor": current_flow.get('jamFactor', 0.0),
                "Confidence": current_flow.get('confidence', 0.0),
                "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        df = pd.DataFrame(locations)
        if df.empty:
            print("Processed DataFrame is empty.")
        return df
    except Exception as e:
        print(f"Error processing traffic data: {e}")
        return pd.DataFrame()

def save_data_to_bigquery(df, table_id):
    """
    Save DataFrame to BigQuery.
    """
    try:
        if df is None or df.empty:
            print("No data to save to BigQuery.")
            return

        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Data saved to BigQuery table {table_id}")
    except Exception as e:
        print(f"Error saving to BigQuery: {e}")

def collect_traffic_data(event=None, context=None):
    """
    Collect traffic data, process it, and save to BigQuery.
    """
    try:
        print(f"Collecting traffic data at {datetime.now()}")
        traffic_data = get_traffic_data()
        if traffic_data is None:
            print("No traffic data fetched from API.")
            return

        df = process_traffic_data(traffic_data)
        if df is None or df.empty:
            print("No processed data to save.")
            return

        save_data_to_bigquery(df, "iot-traffic-444216.traffic_data.01")
    except Exception as e:
        print(f"Unexpected error in collect_traffic_data: {e}")

if _name_ == "_main_":
    collect_traffic_data()