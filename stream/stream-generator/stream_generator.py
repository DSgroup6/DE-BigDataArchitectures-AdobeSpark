import os
import sys
import time
import json
from google.cloud import pubsub
import pandas as pd
from google.cloud import storage

def load_df():

  # storage_client = storage.Client()
  bucket_name = "group6_chicagocrime"
  file_name = 'chicago_crimes.csv'
  # url = 'D:/Datasets/sna/crime_data/chicago_crimes.csv' 
  url = f'gs://{bucket_name}/{file_name}'
  df = pd.read_csv(url)
  # Creates the new bucket
  # bucket = storage_client.create_bucket(bucket_name)
  # blob = bucket.blob()
  # print(f"Bucket {bucket.name} created.")

  # with blob.open("r") as f:
  # 	file = pd.read_csv(f.read())
  return df

PROJECT = 'datatengineering-group6'
TOPIC_NAME = 'crimes'

publisher = pubsub.PublisherClient()
topic_url = 'projects/{project_id}/topics/{topic}'.format(
    project_id=PROJECT,
    topic=TOPIC_NAME,
)


print('loading df...')
df = load_df()

print('df has been loaded')

for i in range(10000):
    crime = df.loc[i].to_json()
    # print(crime)
    publisher.publish(topic_url, crime.encode('utf-8'))
    time.sleep(0.1)


#gcloud pubsub topics create crimes
# gcloud pubsub subscriptions create crimes-subscription --topic=crimes
  
  
  
