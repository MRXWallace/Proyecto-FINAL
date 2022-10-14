import os
from google.cloud import storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="ds-coh3-g17-b58cf05b94c3.json"
from load_kaggle import get_kaggle
storage_client = storage.Client()
bucket_name = "flights-ds03-g17"


# Include in the list the Dataset to download
list_download_kaggle = ['bingecode/us-national-flight-data-2015-2020'
                    ,'sobhanmoosavi/us-weather-events']

for dataset in list_download_kaggle:
    get_kaggle(dataset)

#Ingesting weather_events.csv to Google Cloud Storage

local_file_name = "datasets/WeatherEvents_Jan2016-Dec2021.csv"
file_name_in_google_cloud = "raw_data/weather_events.csv"
#bucket_name = "flights-ds03-g17"

print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")


#Ingesting flights_2020.csv to Google Cloud Storage

local_file_name = "datasets/flights_2020/flights_2020.csv"
file_name_in_google_cloud = "raw_data/flights_2020.csv"
#bucket_name = "flights-ds03-g17"

print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2019.csv to Google Cloud Storage

local_file_name = "datasets/flights_2019/flights_2019.csv"
file_name_in_google_cloud = "raw_data/flights_2019.csv"
#bucket_name = "flights-ds03-g17"

print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2018.csv to Google Cloud Storage

local_file_name = "datasets/flights_2018/flights_2018.csv"
file_name_in_google_cloud = "raw_data/flights_2018.csv"
#bucket_name = "flights-ds03-g17"

print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2017.csv to Google Cloud Storage

local_file_name = "datasets/flights_2017/flights_2017.csv"
file_name_in_google_cloud = "raw_data/flights_2017.csv"
#bucket_name = "flights-ds03-g17"

print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2016.csv to Google Cloud Storage

local_file_name = "datasets/flights_2016/flights_2016.csv"
file_name_in_google_cloud = "raw_data/flights_2016.csv"
#bucket_name = "flights-ds03-g17"

print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

