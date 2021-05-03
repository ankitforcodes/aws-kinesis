import boto3
import json
from datetime import datetime
import time

# namoe of the stream
my_stream_name = 'StockTradeStream' 

# create 'kinesis client' to push data to the kinesis stream
# other credentials like access_key, secret_key is taken from ~/.aws/credentials file
kinesis_client = boto3.client('kinesis', region_name='ap-south-1')

response = kinesis_client.describe_stream(StreamName=my_stream_name)

#print(response['StreamDescription']['Shards'])
my_shard_id = response['StreamDescription']['Shards'][1]['ShardId']

shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name, ShardId=my_shard_id, ShardIteratorType='LATEST')

my_shard_iterator = shard_iterator['ShardIterator']

record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator, Limit=2)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=2)

    print(record_response)

    # wait for 0.2 seconds
    time.sleep(0.2)