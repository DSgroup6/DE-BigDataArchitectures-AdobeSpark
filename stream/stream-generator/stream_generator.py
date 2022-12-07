import time
import pandas as pd
from kafka import KafkaProducer

def load_df():
  bucket_name = "group6_chicagocrime"
  file_name = 'crimes_in_chicago_streamdata.csv'
  # url = f'D:/Datasets/sna/crime_data/{file_name}' 
  url = f'gs://{bucket_name}/{file_name}'
  df = pd.read_csv(url)

  return df

def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)

def success(metadata):
    print(metadata.topic)

def error(exception):
    print(exception)

def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()

print('loading df...')
df = load_df()
print('df has been loaded')
print(f'amount of records: {len(df)}')

producer = KafkaProducer(bootstrap_servers='34.27.65.215:9092')

def success(metadata):
    print(metadata.topic)
def error(exception):
    print(exception)

def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    producer.flush()

print('start sending messages')
for i in range(len(df)-1):
    crime = df.loc[i].to_json()
    print(crime)
    kafka_python_producer_sync(producer, crime, 'crimes')
    
    # publisher.publish(topic_url, crime.encode('utf-8'))
    time.sleep(1)
print('finished sending messages')

# from tutoria: https://cloud.google.com/architecture/using-apache-spark-dstreams-with-dataproc-and-pubsub
#gcloud pubsub topics create crimes
# gcloud pubsub subscriptions create crimes-subscription --topic=crimes
  
  
  

