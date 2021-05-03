import boto3
import json
from datetime import datetime
import calendar
import random
import time
import pandas as pd

# Metohd used 'put_records()'
# name of the 'stream' or 'topic'
my_stream_name = 'StockTradeStream'
# create 'kinesis client' to push data to the kinesis stream
# other credentials like access_key, secret_key is taken from ~/.aws/credentials file
kinesis_client = boto3.client('kinesis', region_name='ap-south-1')


weather_df = pd.read_csv('./datasets/storm_data.csv', usecols = ['STATE', 'YEAR', 'INJURIES_DIRECT', 'DEATHS_DIRECT'])

json_data = json.loads(weather_df.to_json(orient="records"))

# define batch size
# this means that 10 records will be grouped together and send to kinesis at once
BATCH_SIZE = 10

'''
Request Format
kinesis_client.put_records(
    Records=[
        {
            'Data': b'bytes',			 # required
            'ExplicitHashKey': 'string', # optional
            'PartitionKey': 'string'	 # required
        },
    ],
    StreamName='string'
)
'''

bath_count = 0
json_msg_list = []
for data in json_data:
	json_msg = {}
	bath_count += 1
	json_msg['Data'] = json.dumps(data)				# Data field of the record should contain the actual data as a String
	json_msg['PartitionKey'] = str(data["YEAR"])	# PartitionKey field of the record should have partition key logic as String
	json_msg_list.append(json_msg)					# Creating an array of 10 records
	if bath_count % BATCH_SIZE == 0:
		put_multi_response = kinesis_client.put_records(Records = json_msg_list, StreamName = my_stream_name)
		json_msg_list = []