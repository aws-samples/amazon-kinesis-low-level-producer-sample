import boto3


def get_hashkeys_for_open_shards(kinesis, streamname):
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

region_name="us-east-1";
stream_name = "stream_with_10_shards"

kinesis = boto3.client('kinesis', region_name)
result = get_hashkeys_for_open_shards(kinesis, stream_name)
for i in result:
    print(i)
