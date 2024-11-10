import os
import json
import gzip
import zoneinfo
import argparse
import pandas as pd
from io import BytesIO
from minio import Minio
from datetime import datetime

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


def generate_record(entity):
    enriched_stoptime_updates = []

    if 'TripUpdate' not in entity:
        return pd.DataFrame()

    if 'StopTimeUpdate' not in entity['TripUpdate']:
        return pd.DataFrame()

    for key in entity['TripUpdate']['StopTimeUpdate']:
        key['trip_id'] = entity['TripUpdate']['Trip']['TripId']
        key['route_id'] = entity['TripUpdate']['Trip']['RouteId']
        key['start_datetime'] = datetime.strptime(
            entity['TripUpdate']['Trip']['StartDate'] + ' ' +
            entity['TripUpdate']['Trip']['StartTime'],
            '%Y%m%d %H:%M:%S'
        ).replace(tzinfo=zurich_tz).isoformat()
        key['entity_id'] = entity['Id']

        if ':' in key['StopId']:
            key['platform'] = key['StopId'].split(':')[2]

        enriched_stoptime_updates.append(key)

    return pd.json_normalize(enriched_stoptime_updates)


# Load compressed object from S3
response = s3_client.get_object(os.getenv('S3_BUCKET'), args.object_name)

# Decompress the object
compressed_data = response.read()
with gzip.GzipFile(fileobj=BytesIO(compressed_data)) as decompressed_file:
    json_data = json.load(decompressed_file)

frames = []
for entity in json_data['Entity']:
    frames.append(generate_record(entity))

df = pd.concat(frames)

df.to_parquet('stoptime_updates.parquet')

# Upload the parquet file to S3
s3_client.fput_object(
    os.getenv('S3_BUCKET_PARQUET'),
    'stoptime_updates.parquet',
    'stoptime_updates.parquet'
)
