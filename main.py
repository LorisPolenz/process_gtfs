import os
import json
import gzip
import math
import zoneinfo
import argparse
import pandas as pd
from io import BytesIO
from minio import Minio
from datetime import datetime, timedelta

zurich_tz = zoneinfo.ZoneInfo("Europe/Zurich")

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--object-name', type=str, required=True)
args = parser.parse_args()


# Initialize the S3 client
s3_client = Minio(os.getenv('S3_ENDPOINT'),
                  os.getenv('S3_ACCESS_KEY'),
                  os.getenv('S3_SECRET_KEY'),
                  secure=True)


def process_start_datetime(start_date, start_time, trip_id):
    # Can be greater than 24 hours according to GTFS specification
    hour_nr = int(start_time.split(':')[0])

    hours_actual = hour_nr % 24
    days_actual = math.floor(hour_nr / 24)

    # Convert the string to a datetime object
    datetime_day = datetime.strptime(start_date, '%Y%m%d')

    # Add the hours to the datetime objec t
    datetime_day_actual = datetime_day + timedelta(days_actual)

    final_datetime = datetime_day_actual.replace(
        hour=hours_actual,
        minute=int(start_time.split(':')[2]),
        second=int(start_time.split(':')[2]),
        tzinfo=zurich_tz)

    if days_actual > 0:
        print(trip_id, "has a start time greater than 24 hours")

    return final_datetime.isoformat()


def generate_record(entity):
    enriched_stoptime_updates = []

    if 'TripUpdate' not in entity:
        return pd.DataFrame()

    if 'StopTimeUpdate' not in entity['TripUpdate']:
        return pd.DataFrame()

    for key in entity['TripUpdate']['StopTimeUpdate']:
        key['trip_id'] = entity['TripUpdate']['Trip']['TripId']
        key['route_id'] = entity['TripUpdate']['Trip']['RouteId']

        key['start_datetime'] = process_start_datetime(
            entity['TripUpdate']['Trip']['StartDate'],
            entity['TripUpdate']['Trip']['StartTime'],
            key['trip_id']
        )

        key['entity_id'] = entity['Id']

        if ':' in key['StopId']:
            key['platform'] = key['StopId'].split(':')[2]

        enriched_stoptime_updates.append(key)

    return pd.json_normalize(enriched_stoptime_updates)


# Load compressed object from S3
response = s3_client.get_object(
    os.getenv('S3_BUCKET_GTFS_RT'), args.object_name)

# Decompress the object
compressed_data = response.read()
with gzip.GzipFile(fileobj=BytesIO(compressed_data)) as decompressed_file:
    json_data = json.load(decompressed_file)

frames = []
for entity in json_data['Entity']:
    frames.append(generate_record(entity))

df = pd.concat(frames)

parquet_buffer = BytesIO()
df.to_parquet(parquet_buffer)
parquet_buffer.seek(0)

# Upload the parquet file to S3
s3_client.put_object(
    bucket_name=os.getenv('S3_BUCKET_STOPTIME'),
    object_name='stoptime_updates.parquet',
    data=parquet_buffer,
    length=parquet_buffer.getbuffer().nbytes,
    content_type='application/octet-stream'
)
