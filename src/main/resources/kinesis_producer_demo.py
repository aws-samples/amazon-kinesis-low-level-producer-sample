import boto3
from itertools import cycle
import time

# method to get Hash Keys for open shards of a stream
def get_hashkeys_for_open_shards(streamname, region_name):
    kinesis = boto3.client('kinesis', region_name=region_name)
    shard_id = []
    hashkey_list = []
    res = kinesis.list_shards(StreamName=streamname)
    x = res.get("Shards")
    for j in x:
        snr = j.get("SequenceNumberRange")
        if len(snr.keys()) == 1:
            shard_id.append(j.get("ShardId"))
            hkr = j.get("HashKeyRange")
            hashkey_list.append(hkr.get("StartingHashKey"))
        else:
            continue
    return hashkey_list


# method to write a single record to a Kinesis Stream using put_record
def write_single_record(kinesis, streamname, data, hashkey_iterator):
    retry_exceptions = ['ProvisionedThroughputExceededException',
                        'ResourceNotFoundException']
    try:
        response = kinesis.put_record(StreamName=streamname, Data=data, PartitionKey='Awesome',
                                      ExplicitHashKey=next(hashkey_iterator))
        print(response['SequenceNumber'])
    except Exception as e:
        if e.response['Error']['Code'] == retry_exceptions[1]:
            print("Kinesis stream not found: ", streamname)
        elif e.response['Error']['Code'] == retry_exceptions[0]:
            print("ERROR: Throughput Exception Thrown")
            print("Retrying after a short delay")
            time.sleep(0.1)
            response = kinesis.put_record(StreamName=streamname, Data=data, PartitionKey='Awesome',
                                          ExplicitHashKey=next(hashkey_iterator))
        else:
            print("")


# method to write multiple records to a Kinesis Stream using put_records
def write_multiple_records(kinesis, streamname, input_record_list, hashkey_iterator):
    # Partitions the msg list to length 500. Not required if msg list is always less than 500
    def partition(l):
        for i in range(0, len(l), 500):
            yield l[i:i + 500]
    input_record_sub_list = list(partition(input_record_list))

    for msg in input_record_sub_list:
        print("partition")
        records = []
        for j in msg:
            dict = {}
            dict['Data'] = j
            dict['ExplicitHashKey'] = next(hashkey_iterator)
            dict['PartitionKey'] = "Hello"
            records.append(dict)
        try:
            response = kinesis.put_records(Records=records, StreamName=streamname)
            print(response)
            while (response.get('FailedRecordCount') > 0):
                putRecordReqEntry=[]
                print("Processing Rejected Records")
                time.sleep(0.5)
                FailedRecordsList=[]
                putRecsResEntryList = response.get("Records")
                for k in range(0, len(putRecsResEntryList)):
                    if (putRecsResEntryList[j].get("ErrorCode")):
                        FailedRecordsList.append(records[k])

                res = kinesis.put_records(Records=FailedRecordsList, StreamName=streamname)
                print("After Retrying Rejected Records insertion: \n")
                print(res)
        except Exception as e:
            print("Exception in Kinesis Batch Insert: ")


region_name="us-east-1";
stream_name = "stream_with_10_shards"

# get Hash Keys for open shards of a stream
hashkey_list = get_hashkeys_for_open_shards(stream_name, region_name)

# prepare a round-robin list of Hash Keys
hashkey_iterator = cycle(hashkey_list)

# get Kinesis client
kinesis = boto3.client('kinesis')

# prepare input records
input_record_list = []
for i in range(1, 2005):
    input_record_list.append("Hello World - PutRecords with Explicit Hash Key!!")


# write a single records to Kinesis stream
write_single_record(kinesis, stream_name, 'Hello World - PutRecord with Explicit Hash Key!!', hashkey_iterator)

# write multiple records to Kinesis stream
write_multiple_records(kinesis, stream_name, input_record_list, hashkey_iterator)
