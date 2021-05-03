import boto3
import json
from datetime import datetime
import calendar
import random
import time

my_stream_name = 'StockTradeStream'
kinesis_client = boto3.client('kinesis', region_name='ap-south-1')

'''
Method used 'put_record()'
Put data in Kinesis record by record
Generally avoided as there is an upper limit of 1000/sec msgs per shard (or 1Mbps per shard)
'''

'''
client.put_record(
    StreamName='string',				# required: Name of the Stream in which data is to be sent
    Data=b'bytes',						# required: Actual record to ber sent to a stream
    PartitionKey='string',				# required: Determines which shard the data will go to
    ExplicitHashKey='string',			# optional: If partioning needs to be done on endocing else than MD5
    SequenceNumberForOrdering='string'	# optional: If we want the sequence number of records to strictly increase
)
'''

def put_single_record_to_stream(thing_id, property_value, property_timestamp):
    payload = {'prop': str(property_value), 'timestamp': str(property_timestamp), 'thing_id': thing_id}
    
    # Writes a single data record into an Amazon Kinesis data stream
    # Each shard can support writes up to 1,000 records per second
    # up to a maximum data write total of 1 MiB per second
    # Partition Key has maximum length limit of 256 characters
    # PutRecord returns the shard ID of where the data record was placed and the sequence number that was assigned to the data record
    put_response = kinesis_client.put_record(StreamName=my_stream_name, Data=json.dumps(payload), PartitionKey=str(property_value))


single_records = 0
while True:
	single_records += 1
	print("RECORDS SENT: ", single_records)
	property_value = random.randint(40, 120)
	property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
	thing_id = 'aa-bb'

	put_single_record_to_stream(thing_id, property_value, property_timestamp)

	# wait for 0.1 secs
	time.sleep(0.100)
	# send 100 records, then stop
	if single_records == 100:
		break
