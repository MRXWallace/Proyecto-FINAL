import os
from google.cloud import storage
from google.cloud import bigquery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="ETL/ds-coh3-g17-b58cf05b94c3.json"
from load_kaggle import get_kaggle
import pandas as pd 

storage_client = storage.Client()
client = bigquery.Client()
bucket_name = "flights-ds03-g17"
dataset_id = 'flights_project'


# Include in the list the Dataset to download
list_download_kaggle = ['bingecode/us-national-flight-data-2015-2020'
                    ,'sobhanmoosavi/us-weather-events']

for dataset in list_download_kaggle:
    get_kaggle(dataset)

#Transform weather_events 
df = pd.read_csv('datasets/WeatherEvents_Jan2016-Dec2021.csv')
df['StartTime(UTC)']= pd.to_datetime(df['StartTime(UTC)'], format='%Y-%m-%d')
df['StartDateOnly'] = [d.date() for d in df['StartTime(UTC)']]
df['StartTimeOnly'] = [d.time() for d in df['StartTime(UTC)']]
df['EndTime(UTC)']= pd.to_datetime(df['EndTime(UTC)'], format='%Y-%m-%d')
df['EndDateOnly'] = [d.date() for d in df['EndTime(UTC)']]
df['EndTimeOnly'] = [d.time() for d in df['EndTime(UTC)']]
df.to_csv('datasets/WeatherEvents.csv',encoding='utf-8',index=False)




#Ingesting weather_events.csv to Google Cloud Storage

local_file_name = "datasets/WeatherEvents.csv"
file_name_in_google_cloud = "Data/WeatherEvents.csv"

print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting all_flights.csv to Google Cloud Storage

local_file_name = "datasets/flights/flights_2020.csv"
file_name_in_google_cloud = "Data/all_flights.csv"

print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2020.csv to Google Cloud Storage

local_file_name = "datasets/flights_2020/flights_2020.csv"
file_name_in_google_cloud = "Data/2020_flights.csv"


print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2019.csv to Google Cloud Storage

local_file_name = "datasets/flights_2019/flights_2019.csv"
file_name_in_google_cloud = "Data/2019_flights.csv"


print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2018.csv to Google Cloud Storage

local_file_name = "datasets/flights_2018/flights_2018.csv"
file_name_in_google_cloud = "Data/2018_flights.csv"


print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2017.csv to Google Cloud Storage

local_file_name = "datasets/flights_2017/flights_2017.csv"
file_name_in_google_cloud = "Data/2017_flights.csv"


print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2016.csv to Google Cloud Storage

local_file_name = "datasets/flights_2016/flights_2016.csv"
file_name_in_google_cloud = "Data/2016_flights.csv"


print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Ingesting flights_2015.csv to Google Cloud Storage

local_file_name = "datasets/flights_2015/flights_2015.csv"
file_name_in_google_cloud = "Data/flights_2015.csv"


print("Uploading dataset to Data Lake...")
bucket = storage_client.bucket(bucket_name)
cloud_file_blob = bucket.blob(file_name_in_google_cloud)
cloud_file_blob.upload_from_filename(local_file_name,timeout=(120,400))
uri = "gs://" + bucket_name + "/" + file_name_in_google_cloud
print("File blob uri: "  + uri)
print("Finished upload.")

#Create Tables BQ 

#Create table weather_events
table_id = "ds-coh3-g17.flights_project.weather_events"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Create table all_flights
table_id = "ds-coh3-g17.flights_project.all_flights"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Create table flights_2020
table_id = "ds-coh3-g17.flights_project.2020_flights"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Create table flights_2019
table_id = "ds-coh3-g17.flights_project.2019_flights"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Create table flights_2018
table_id = "ds-coh3-g17.flights_project.2018_flights"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Create table flights_2017
table_id = "ds-coh3-g17.flights_project.2017_flights"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Create table flights_2016
table_id = "ds-coh3-g17.flights_project.2016_flights"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

