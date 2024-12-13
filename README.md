# Iotproject

README
Access to Traffic Data by Hour
If you would like permission to access the raw traffic data by hour stored in BigQuery, please contact me directly at baltayevaa07@gmail.com. Unfortunately, due to Google Cloud Platform (GCP) limitations, I am unable to provide a direct link to the dataset.

Transformed Data
The transformed datasets are available in this repository and include:

clustered_traffic_with_names.csv: Traffic data with clusters applied.
air_quality_by_cluster.csv: Air quality data mapped to clusters.
weather_london.csv: Historical weather data for London.
These files have been processed and transformed for analysis and visualization purposes.

Explanation of Scripts
data.py
This script is used to collect data from the HERE Traffic API and save it for further processing. It automates hourly data retrieval and storage for the raw traffic dataset.

wat.py
This script handles the upload and organization of processed datasets, including those saved in this repository.

data_openwether.py
This script interacts with the OpenWeather API to fetch weather and air quality data for the London region. It processes the responses and prepares the data for integration with traffic and air quality datasets.

Notes on BigQuery Storage
The raw traffic data, prior to clustering and transformation, exceeds 1 GB in size and is therefore stored in BigQuery. This approach ensures scalability and efficient querying for large-scale datasets. If you require access to this data, please reach out via email as mentioned above.

For my dashboard you have find it here https://lookerstudio.google.com/s/nUx94-OOpGE
