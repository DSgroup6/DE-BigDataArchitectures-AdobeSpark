import os
import sys
import time
import json
from google.cloud import pubsub
import pandas as pd
# from google.cloud import storage
import datetime as dt
def load_df():
  bucket_name = "group6_chicagocrime"
  file_name = 'chicago_crimes2.csv'
  # url = f'D:/Datasets/sna/crime_data/{file_name}' 
  url = f'gs://{bucket_name}/{file_name}'
  df = pd.read_csv(url)

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
print(f'amount of records: {len(df)}')

print('start sending messages')
for i in range(len(df)-1):
    crime = df.loc[i].to_json()
    # print(crime)
    publisher.publish(topic_url, crime.encode('utf-8'))
    time.sleep(0.1)
print('finished sending messages')

# from tutoria: https://cloud.google.com/architecture/using-apache-spark-dstreams-with-dataproc-and-pubsub
#gcloud pubsub topics create crimes
# gcloud pubsub subscriptions create crimes-subscription --topic=crimes
  
  
  

# df['Date'] = pd.to_datetime(df['Date'])
# print('sorting...')

# df = df[df['Date'].dt.year == 2015]
# print('only subset selected')

# df = df.sort_values(by ='Date',inplace=False)

# df.to_csv('D:/Datasets/sna/crime_data/chicago_crimes2.csv')