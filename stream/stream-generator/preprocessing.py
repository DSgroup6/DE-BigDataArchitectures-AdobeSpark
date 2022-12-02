import time
import pandas as pd
from kafka import KafkaProducer

def load_df():
  bucket_name = "group6_chicagocrime"
  file_name = 'chicago_crimes2.csv'
  url = f'D:/Datasets/sna/crime_data/{file_name}' 
  # url = f'gs://{bucket_name}/{file_name}'
  df = pd.read_csv(url)

  return df

df = load_df()

df['Date'] = pd.to_datetime(df['Date'])
print('sorting...')

df = df[df['Date'].dt.year == 2015]


df = df[['Community Area','Primary Type','Case Number','Date']]
print('only subset selected')

df = df.sort_values(by ='Date',inplace=False)

df.to_csv('D:/Datasets/sna/crime_data/chicago_crimes3.csv')