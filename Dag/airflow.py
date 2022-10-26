from __future__ import print_function
import pandas as pd
import requests,os
import datetime
from socket import timeout
from time import time
from urllib3 import Retry
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
import pandas as pd
#from common import pull_stocks, pull_fin

default_dag_args = {
    'owner': 'Fajardo',
    'start_date': datetime.datetime(2022,10,18),
    'retries': 0,
}

#weather_data_path = '/home/christiancarlosf/Desktop/g17/datasets/WeatherEvents_Jan2016-Dec2021.csv'
#transform_data_output_path = '/home/christiancarlosf/Desktop/g17/datasets/Transformed_WeatherEvents_Jan2016-Dec2021.csv'
#def load_transform_data(weather_data_path,transform_data_output_path):
#  df = pd.read_csv(weather_data_path)
#  df['StartTime(UTC)']= pd.to_datetime(df['StartTime(UTC)'], format='%Y-%m-%d')
#  df['StartDateOnly'] = [d.date() for d in df['StartTime(UTC)']]
#  df['StartTimeOnly'] = [d.time() for d in df['StartTime(UTC)']]
#  df['EndTime(UTC)']= pd.to_datetime(df['EndTime(UTC)'], format='%Y-%m-%d')
#  df['EndDateOnly'] = [d.date() for d in df['EndTime(UTC)']]
#  df['EndTimeOnly'] = [d.time() for d in df['EndTime(UTC)']]
#  return df.to_csv(transform_data_output_path,encoding='utf-8',index=False)


# params
gs_bucket = "flights-ds03-g17"
project_id = "ds-coh3-g17"
staging_dataset = "flights_project"
dago = models.DAG(
        'Load_project_data',
        schedule_interval=datetime.timedelta(days=1),
        catchup=False,
        start_date=datetime.datetime.now(),
        max_active_runs=1,
        default_args=default_dag_args)

#transform_data = PythonOperator(   
#  task_id='transform_data',
#  python_callable=load_transform_data,
#  op_kwargs={'weather_data_path': weather_data_path,
#  'transform_data_output_path': transform_data_output_path},
#)
#stored_data_gcs = LocalFilesystemToGCSOperator(
##        task_id="store_to_gcs",
 #       src="/home/christiancarlosf/Desktop/g17/datasets/flights/flights.csv",
#        dst="raw_data/flights.csv",
#        bucket= gs_bucket
#)
load_weather_events = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_weather_events',
    bucket = gs_bucket,
    source_objects = ['Data/WeatherEvents.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.events_weather',
    source_format = 'csv',
    skip_leading_rows = 1,
    autodetect=True,
    field_delimiter=',',
    dag=dago,
    write_disposition='WRITE_TRUNCATE',
     #schema_fields=[
       # {'name': 'EventId', 'type': 'STRING', 'mode': 'NULLABLE'},
        #{'name': 'Type', 'type': 'STRING', 'mode': 'NULLABLE'},
        #{'name': 'Severity', 'type': 'STRING', 'mode': 'NULLABLE'},
        ##{'name': 'StartTime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        #{'name': 'EndTime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        #{'name': 'Precipitation', 'type': 'STRING', 'mode': 'NULLABLE'},
        #{'name': 'TimeZone', 'type': 'STRING', 'mode': 'NULLABLE'},
        #{'name': 'AirportCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        #{'name': 'X', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #{'name': 'Y', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #{'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'},
        #{'name': 'County', 'type': 'STRING', 'mode': 'NULLABLE'},
        #{'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        #{'name': 'ZipCode', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #]
)
load_all_flights = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_all_flights',
    bucket = gs_bucket,
    source_objects = ['Data/all_flights.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.all_flights_1',
    source_format = 'csv',
    #skip_leading_rows = 1,
    autodetect=False,
    field_delimiter=',',
    dag=dago,
    write_disposition='WRITE_TRUNCATE',
     schema_fields=[
        {'name': 'YEAR', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_WEEK', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'OP_UNIQUE_CARRIER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_DEP_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEP_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_ARR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ARR_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CANCELLED', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'CANCELLATION_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AIR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DISTANCE', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ]
)
load_2020_flights = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_2020_flights',
    bucket = gs_bucket,
    source_objects = ['Data/2020_flights.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.2020_flights',
    source_format = 'csv',
    #skip_leading_rows = 1,
    autodetect=False,
    field_delimiter=',',
    dag=dago,
    write_disposition='WRITE_TRUNCATE',
     schema_fields=[
        {'name': 'YEAR', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_WEEK', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'OP_UNIQUE_CARRIER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_DEP_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEP_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_ARR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ARR_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CANCELLED', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'CANCELLATION_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AIR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DISTANCE', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ]
)
load_2019_flights = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_2019_flights',
    bucket = gs_bucket,
    source_objects = ['Data/2019_flights.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.2019_flights',
    source_format = 'csv',
    #skip_leading_rows = 1,
    autodetect=False,
    field_delimiter=',',
    dag=dago,
    write_disposition='WRITE_TRUNCATE',
     schema_fields=[
        {'name': 'YEAR', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_WEEK', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'OP_UNIQUE_CARRIER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_DEP_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEP_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_ARR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ARR_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CANCELLED', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'CANCELLATION_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AIR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DISTANCE', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ]
)
load_2018_flights = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_2018_flights',
    bucket = gs_bucket,
    source_objects = ['Data/2018_flights.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.2018_flights',
    source_format = 'csv',
    #skip_leading_rows = 1,
    autodetect=False,
    field_delimiter=',',
    dag=dago,
    write_disposition='WRITE_TRUNCATE',
     schema_fields=[
        {'name': 'YEAR', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_WEEK', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'OP_UNIQUE_CARRIER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_DEP_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEP_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_ARR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ARR_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CANCELLED', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'CANCELLATION_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AIR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DISTANCE', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ]
)
load_2017_flights = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_2017_flights',
    bucket = gs_bucket,
    source_objects = ['Data/2017_flights.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.2017_flights',
    source_format = 'csv',
    #skip_leading_rows = 1,
    autodetect=False,
    field_delimiter=',',
    dag=dago,
    write_disposition='WRITE_TRUNCATE',
     schema_fields=[
        {'name': 'YEAR', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_WEEK', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'OP_UNIQUE_CARRIER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_DEP_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEP_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_ARR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ARR_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CANCELLED', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'CANCELLATION_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AIR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DISTANCE', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ]
)
load_2016_flights = GoogleCloudStorageToBigQueryOperator(
    task_id = 'load_2016_flights',
    bucket = gs_bucket,
    source_objects = ['Data/2016_flights.csv'],
    destination_project_dataset_table = f'{project_id}:{staging_dataset}.2016_flights',
    source_format = 'csv',
    #skip_leading_rows = 1,
    autodetect=False,
    field_delimiter=',',
    dag=dago,
    write_disposition='WRITE_TRUNCATE',
     schema_fields=[
        {'name': 'YEAR', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_MONTH', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DAY_OF_WEEK', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'OP_UNIQUE_CARRIER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORIGIN_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_CITY_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEST_STATE_ABR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_DEP_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DEP_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CRS_ARR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ARR_DELAY_NEW', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CANCELLED', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'CANCELLATION_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AIR_TIME', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DISTANCE', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ]
)



#transform_data >> 
load_weather_events >>  load_all_flights >> load_2020_flights >> load_2019_flights >> load_2018_flights >> load_2017_flights >> load_2016_flights 







