import pandas as pd
from sklearn.cluster import KMeans
from google.cloud import bigquery

# Set up BigQuery client
client = bigquery.Client()

# Query to retrieve data from BigQuery
def fetch_data_from_bigquery(table_id):
    query = f"""
    SELECT
        Description,
        Latitude,
        Longitude,
        Speed,
        Free_Flow,
        Jam_Factor,
        Confidence,
        Timestamp
    FROM `{table_id}`
    WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
    """
    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

# Apply K-Means clustering
def apply_kmeans_clustering(df, n_clusters):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df['Cluster'] = kmeans.fit_predict(df[['Latitude', 'Longitude']])
    # Add cluster centroids for reference
    df['Cluster_Center_Lat'] = df['Cluster'].map(lambda c: kmeans.cluster_centers_[c][0])
    df['Cluster_Center_Lon'] = df['Cluster'].map(lambda c: kmeans.cluster_centers_[c][1])
    return df

# Save the clustered data to a CSV file
def save_to_csv(df, output_path):
    df.to_csv(output_path, index=False)
    print(f"Clustered data saved to {output_path}")

if __name__ == "__main__":
    # Parameters
    table_id = "iot-traffic-444216.traffic_data.01"
    output_csv = "clustered_traffic_data.csv"

    # Step 1: Fetch data from BigQuery
    print("Fetching data from BigQuery...")
    traffic_data = fetch_data_from_bigquery(table_id)

    # Step 2: Apply K-Means clustering
    print("Applying K-Means clustering...")
    clustered_data = apply_kmeans_clustering(traffic_data, n_clusters=32)

    # Step 3: Save clustered data to CSV
    print("Saving clustered data to CSV...")
    save_to_csv(clustered_data, output_csv)
    print("Processing complete.")


import requests
import pandas as pd
from datetime import datetime, timedelta

# # OpenWeather API configuration
# API_KEY = "YOUR_API_KEY"  # Replace with your OpenWeather API key
LAT = 51.509865  # Latitude for London
LON = -0.118092  # Longitude for London
BASE_URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"

# Date range for historical data
START_DATE = datetime(2024, 12, 3)
END_DATE = datetime(2024, 12, 11)

def fetch_air_quality(lat, lon, start, end):
    """
    Fetch historical air quality data for a specific location and date range.
    """
    params = {
        "lat": lat,
        "lon": lon,
        "start": int(start.timestamp()),
        "end": int(end.timestamp()),
        "appid": API_KEY,
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def main():
    current_date = START_DATE
    delta = timedelta(days=1)
    all_data = []

    while current_date <= END_DATE:
        start = current_date
        end = current_date + delta - timedelta(seconds=1)  # Fetch for one day
        print(f"Fetching air quality data for {start.strftime('%Y-%m-%d')}...")

        data = fetch_air_quality(LAT, LON, start, end)
        if data and "list" in data:
            for entry in data["list"]:
                row = {
                    "DateTime": datetime.fromtimestamp(entry["dt"]),
                    "AQI": entry["main"]["aqi"],
                    "CO": entry["components"]["co"],
                    "NO": entry["components"]["no"],
                    "NO2": entry["components"]["no2"],
                    "O3": entry["components"]["o3"],
                    "SO2": entry["components"]["so2"],
                    "PM2_5": entry["components"]["pm2_5"],
                    "PM10": entry["components"]["pm10"],
                    "NH3": entry["components"]["nh3"],
                }
                all_data.append(row)

        current_date += delta

    # Convert to DataFrame and save as CSV
    air_quality_df = pd.DataFrame(all_data)
    csv_file_path = "historical_air_quality_london.csv"
    air_quality_df.to_csv(csv_file_path, index=False)

    print(f"Air quality data saved to {csv_file_path}")
    print(air_quality_df.head())

if __name__ == "__main__":
    main()

 # Cluster centroids
    clusters = [
        {"Cluster": 0, "Lat": 51.52741648, "Lon": -0.171894668},
        {"Cluster": 1, "Lat": 51.55038249, "Lon": -0.056029819},
        {"Cluster": 2, "Lat": 51.47520383, "Lon": -0.066515764},
        {"Cluster": 3, "Lat": 51.58649937, "Lon": -0.102695244},
        {"Cluster": 4, "Lat": 51.50164275, "Lon": -0.226449871},
        {"Cluster": 5, "Lat": 51.41990326, "Lon": -0.132252969},
        {"Cluster": 6, "Lat": 51.51543276, "Lon": -0.013888757},
        {"Cluster": 7, "Lat": 51.53684356, "Lon": -0.203534601},
        {"Cluster": 8, "Lat": 51.45930034, "Lon": -0.158237455},
        {"Cluster": 9, "Lat": 51.53383259, "Lon": -0.104902444},
        {"Cluster": 10, "Lat": 51.54637324, "Lon": 0.021645701},
        {"Cluster": 11, "Lat": 51.49701079, "Lon": -0.273305313},
        {"Cluster": 12, "Lat": 51.57607147, "Lon": -0.012488572},
        {"Cluster": 13, "Lat": 51.43416384, "Lon": -0.041194926},
        {"Cluster": 14, "Lat": 51.4976624, "Lon": 0.043132414},
        {"Cluster": 15, "Lat": 51.50038718, "Lon": -0.09753036},
        {"Cluster": 16, "Lat": 51.58287273, "Lon": -0.224630439},
        {"Cluster": 17, "Lat": 51.60464597, "Lon": -0.145382608},
        {"Cluster": 18, "Lat": 51.49217655, "Lon": -0.178243102},
        {"Cluster": 19, "Lat": 51.54207525, "Lon": -0.260652085},
        {"Cluster": 20, "Lat": 51.47387628, "Lon": -0.022618481},
        {"Cluster": 21, "Lat": 51.46678476, "Lon": 0.019377849},
        {"Cluster": 22, "Lat": 51.46193011, "Lon": -0.111476269},
        {"Cluster": 23, "Lat": 51.46079349, "Lon": -0.253551772},
        {"Cluster": 24, "Lat": 51.58770652, "Lon": -0.184415644},
        {"Cluster": 25, "Lat": 51.41927546, "Lon": -0.08746151},
        {"Cluster": 26, "Lat": 51.55628087, "Lon": -0.141352535},
        {"Cluster": 27, "Lat": 51.50240046, "Lon": -0.133398658},
        {"Cluster": 28, "Lat": 51.4646432, "Lon": -0.206320419},
        {"Cluster": 29, "Lat": 51.4253511, "Lon": -0.185020404},
        {"Cluster": 30, "Lat": 51.60169691, "Lon": -0.060136819},
        {"Cluster": 31, "Lat": 51.51732139, "Lon": -0.063849728},
    ]

    def fetch_air_quality(lat, lon, start, end):
        """
        Fetch historical air quality data for a specific location and date range.
        """
        params = {
            "lat": lat,
            "lon": lon,
            "start": int(start.timestamp()),
            "end": int(end.timestamp()),
            "appid": API_KEY,
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching data for Lat: {lat}, Lon: {lon}: {response.status_code}")
            return None

    def main():
        delta = timedelta(days=1)
        all_data = []

        for cluster in clusters:
            cluster_id = cluster["Cluster"]
            lat = cluster["Lat"]
            lon = cluster["Lon"]

            current_date = START_DATE
            while current_date <= END_DATE:
                start = current_date
                end = current_date + delta - timedelta(seconds=1)  # Fetch for one day
                print(f"Fetching air quality data for Cluster {cluster_id}, Date: {start.strftime('%Y-%m-%d')}...")

                data = fetch_air_quality(lat, lon, start, end)
                if data and "list" in data:
                    for entry in data["list"]:
                        row = {
                            "Cluster": cluster_id,
                            "DateTime": datetime.fromtimestamp(entry["dt"]),
                            "AQI": entry["main"]["aqi"],
                            "CO": entry["components"]["co"],
                            "NO": entry["components"]["no"],
                            "NO2": entry["components"]["no2"],
                            "O3": entry["components"]["o3"],
                            "SO2": entry["components"]["so2"],
                            "PM2_5": entry["components"]["pm2_5"],
                            "PM10": entry["components"]["pm10"],
                            "NH3": entry["components"]["nh3"],
                            "Cluster_Center_Lat": lat,
                            "Cluster_Center_Lon": lon,
                        }
                        all_data.append(row)

                current_date += delta

        # Convert to DataFrame and save as CSV
        air_quality_df = pd.DataFrame(all_data)
        csv_file_path = "historical_air_quality_by_cluster.csv"
        air_quality_df.to_csv(csv_file_path, index=False)

        print(f"Air quality data saved to {csv_file_path}")
        print(air_quality_df.head())

    if __name__ == "__main__":
        main()

        