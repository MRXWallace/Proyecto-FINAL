from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="ds-coh3-g17-b58cf05b94c3.json"
client = bigquery.Client()
dataset_id = 'flights'


#Create table weather_events
table_id = "ds-coh3-g17.flights.weather_events"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Load weather_events.csv into weather_events
table_name = 'weather_events'
uri = "gs://flights-ds03-g17/raw_data/weather_events.csv"
dataset_ref = client.dataset(dataset_id)
table = dataset_ref.table(table_name)
table_ref = client.get_table(table)
schema = [
    bigquery.SchemaField("EventId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Severity", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("StartTime", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("EndTime", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("Precipitation", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("TimeZone", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("AirportCode", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("X", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Y", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("City", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("County", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("State", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ZipCode", "FLOAT", mode="NULLABLE")
    
]
job_config = bigquery.LoadJobConfig(schema=schema)
job_config.job_config.field_delimiter = ","
job_config.skip_leading_rows = 1
load_job = client.load_table_from_uri(
    uri, dataset_ref.table(table_name), job_config=job_config
    )  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
    
print("Job finished.")

#Create table flights_2020
table_id = "ds-coh3-g17.flights.flights_2020"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Load flights_2020.csv into flights_2020
table_name = 'flights_2020'
uri = "gs://flights-ds03-g17/raw_data/flights_2020.csv"
dataset_ref = client.dataset(dataset_id)
table = dataset_ref.table(table_name)
table_ref = client.get_table(table)
schema = [
    bigquery.SchemaField("YEAR", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_WEEK", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("OP_UNIQUE_CARRIER", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_DEP_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEP_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_ARR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ARR_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLED", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLATION_CODE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("AIR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DISTANCE", "INTEGER", mode="NULLABLE")
]
job_config = bigquery.LoadJobConfig(schema=schema)
job_config.field_delimiter = ","
#skip_leading_rows = 1
load_job = client.load_table_from_uri(
    uri, dataset_ref.table(table_name), job_config=job_config
    )  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
    
print("Job finished.")

#Create table flights_2019
table_id = "ds-coh3-g17.flights.flights_2019"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Load flights_2019.csv into flights_2019
table_name = 'flights_2019'
uri = "gs://flights-ds03-g17/raw_data/flights_2019.csv"
dataset_ref = client.dataset(dataset_id)
table = dataset_ref.table(table_name)
table_ref = client.get_table(table)
schema = [
    bigquery.SchemaField("YEAR", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_WEEK", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("OP_UNIQUE_CARRIER", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_DEP_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEP_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_ARR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ARR_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLED", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLATION_CODE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("AIR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DISTANCE", "INTEGER", mode="NULLABLE")
]
job_config = bigquery.LoadJobConfig(schema=schema)
job_config.field_delimiter = ","
#skip_leading_rows = 1
load_job = client.load_table_from_uri(
    uri, dataset_ref.table(table_name), job_config=job_config
    )  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
    
print("Job finished.")

#Create table flights_2018
table_id = "ds-coh3-g17.flights.flights_2018"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Load flights_2018.csv into flights_2018
table_name = 'flights_2018'
uri = "gs://flights-ds03-g17/raw_data/flights_2018.csv"
dataset_ref = client.dataset(dataset_id)
table = dataset_ref.table(table_name)
table_ref = client.get_table(table)
schema = [
    bigquery.SchemaField("YEAR", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_WEEK", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("OP_UNIQUE_CARRIER", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_DEP_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEP_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_ARR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ARR_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLED", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLATION_CODE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("AIR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DISTANCE", "INTEGER", mode="NULLABLE")
]
job_config = bigquery.LoadJobConfig(schema=schema)
job_config.field_delimiter = ","
#skip_leading_rows = 1
load_job = client.load_table_from_uri(
    uri, dataset_ref.table(table_name), job_config=job_config
    )  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
    
print("Job finished.")

#Create table flights_2017
table_id = "ds-coh3-g17.flights.flights_2017"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Load flights_2017.csv into flights_2017
table_name = 'flights_2017'
uri = "gs://flights-ds03-g17/raw_data/flights_2017.csv"
dataset_ref = client.dataset(dataset_id)
table = dataset_ref.table(table_name)
table_ref = client.get_table(table)
schema = [
    bigquery.SchemaField("YEAR", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_WEEK", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("OP_UNIQUE_CARRIER", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_DEP_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEP_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_ARR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ARR_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLED", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLATION_CODE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("AIR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DISTANCE", "INTEGER", mode="NULLABLE")
]
job_config = bigquery.LoadJobConfig(schema=schema)
job_config.field_delimiter = ","
#skip_leading_rows = 1
load_job = client.load_table_from_uri(
    uri, dataset_ref.table(table_name), job_config=job_config
    )  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
    
print("Job finished.")

#Create table flights_2016
table_id = "ds-coh3-g17.flights.flights_2016"
table = bigquery.Table(table_id)
table = client.create_table(table,exists_ok = True)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Load flights_2016.csv into flights_2016
table_name = 'flights_2016'
uri = "gs://flights-ds03-g17/raw_data/flights_2016.csv"
dataset_ref = client.dataset(dataset_id)
table = dataset_ref.table(table_name)
table_ref = client.get_table(table)
schema = [
    bigquery.SchemaField("YEAR", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_MONTH", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("DAY_OF_WEEK", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("OP_UNIQUE_CARRIER", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ORIGIN_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_CITY_NAME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEST_STATE_ABR", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_DEP_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DEP_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CRS_ARR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ARR_DELAY_NEW", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLED", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("CANCELLATION_CODE", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("AIR_TIME", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("DISTANCE", "INTEGER", mode="NULLABLE")
]
job_config = bigquery.LoadJobConfig(schema=schema)
job_config.field_delimiter = ","
#skip_leading_rows = 1
load_job = client.load_table_from_uri(
    uri, dataset_ref.table(table_name), job_config=job_config
    )  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
    
print("Job finished.")

#Create table flights_2015
#table_id = "ds-coh3-g17.flights.flights_2015"
#table = bigquery.Table(table_id)
#table = client.create_table(table,exists_ok = True)  # Make an API request.
#print(
#    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

#Load flights_2015.csv into flights_2015
#table_name = 'flights_2015'
#uri = "gs://flights-ds03-g17/client_python/airlines-Report-Departures.csv"
#dataset_ref = client.dataset(dataset_id)
#table = dataset_ref.table(table_name)
#table_ref = client.get_table(table)
#schema = [
#    bigquery.SchemaField("YEAR", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("MONTH", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("DAY_OF_MONTH", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("DAY_OF_WEEK", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("OP_UNIQUE_CARRIER", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("ORIGIN_CITY_NAME", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("ORIGIN_STATE_ABR", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("DEST_CITY_NAME", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("DEST_STATE_ABR", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("CRS_DEP_TIME", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("DEP_DELAY_NEW", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("CRS_ARR_TIME", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("ARR_DELAY_NEW", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("CANCELLED", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("CANCELLATION_CODE", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("AIR_TIME", "INTEGER", mode="NULLABLE"),
#    bigquery.SchemaField("DISTANCE", "INTEGER", mode="NULLABLE")
#]
#job_config = bigquery.LoadJobConfig(schema=schema)
#job_config.field_delimiter = ","
#skip_leading_rows = 1
#load_job = client.load_table_from_uri(
#    uri, dataset_ref.table(table_name), job_config=job_config
#    )  # API request
#print("Starting job {}".format(load_job.job_id))
#
#load_job.result()  # Waits for table load to complete.
    
#print("Job finished.")
















