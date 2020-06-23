// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.kinesis.blog.demo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * <p>
 * Class with Java main method. It demonstrates how to write data to Kinesis
 * Steam using Hash Keys belong to the shards of the Steam.
 * <p>
 * 
 * @author Ravi Itha, Amazon Web Service, Inc.
 *
 */
public class KinesisProducerDemo {

	public static void main(String[] args) {

		String region = "us-east-1";
		String streamName = "stream_with_500";
		AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();
		KinesisStreamUtil kdsUtil = new KinesisStreamUtil();

		/**
		 * Step 1: Get starting Hash keys for active shards
		 */
		List<String> hashKeysList = kdsUtil.getHashKeysForOpenShards(kinesis, streamName);

		/**
		 * Step 2: Create a circular list of Hash Keys which gives a round-robin effect.
		 */
		Iterator<String> hashKeysIterator = Iterables.cycle(hashKeysList).iterator();

		/**
		 * Step 3: Write records to Kinesis using PutRecords
		 */
		List<String> recordList = getSampleRecords(20000);
		System.out.println("Number of messages to be written: " + recordList.size());
		writeMessagesToKinesis(recordList, streamName, kinesis, hashKeysIterator);

		/**
		 * Write records to Kinesis using PutRecord
		 */
		List<String> sampleRecord = getSampleRecords(1);
		writeSingleMessageToKinesis(sampleRecord.get(0), streamName, kinesis, hashKeysIterator);

	}

	/**
	 * Prepare a list of sample records
	 * @param numRecords
	 * @return List<String>
	 */
	public static List<String> getSampleRecords(int numRecords) {
		List<String> recordList = new ArrayList<String>();
		String jsonString1 = "{ \"glossary\": { \"unique_id\": ";
		String jsonString2 = "\"title\": \"example glossary\", \"GlossDiv\": { \"title\": \"S\", \"GlossList\": "
				+ "{ \"GlossEntry\": { \"ID\": \"SGML\", \"SortAs\": \"SGML\", \"GlossTerm\": \"Standard Generalized Markup Language\", "
				+ "\"Acronym\": \"SGML\", \"Abbrev\": \"ISO 8879:1986\", \"GlossDef\": { \"para\": \"A meta-markup language, used to "
				+ "create markup languages such as DocBook.\", \"GlossSeeAlso\": [\"GML\", \"XML\"] }, \"GlossSee\": \"markup\" } } } } }";
		for (int i = 1; i < numRecords + 1; i++) {
			String s = jsonString1.concat("\"").concat(Integer.toString(i)).concat("\"").concat(",").concat(jsonString2)
					.concat("\n");
			recordList.add(s);
		}
		return recordList;
	}
	
	/**
	 * This method demonstrates writing a single messages to Kinesis Data Stream
	 * using PutRecord API.
	 * 
	 * Explicit Hash Keys: Hash Keys belong to Shards are used to write records.
	 * Partition key is needed and it can be an empty string. When both Partition
	 * Key and explicit Hash Key are set, explicit Hash Key takes precedence.
	 * Calling hashKeyIterator.next() provides a Hash Key belongs to a shard.
	 * 
	 * Retry logic: PutRecord throws ProvisionedThroughputExceededException when a stream 
	 * is throttled. The retry logic used here handles the exception and re-writes the failed 
	 * record.
	 * 
	 * @param record
	 * @param streamName
	 * @param kinesis
	 * @param hashKeyIterator
	 */
	public static void writeSingleMessageToKinesis(String record, String streamName, AmazonKinesis kinesis,
			Iterator<String> hashKeyIterator) {
		PutRecordRequest putRecReq = new PutRecordRequest();
		try {
			putRecReq.setStreamName(streamName);
			putRecReq.setData(ByteBuffer.wrap(record.getBytes()));
			putRecReq.setPartitionKey("reqiredButHasNoEffect-when-setExplicitHashKey-isUsed");
			putRecReq.setExplicitHashKey(hashKeyIterator.next());
			kinesis.putRecord(putRecReq);
		} catch (ProvisionedThroughputExceededException exception) {
			try {
				System.out.println("ERROR: Throughput Exception Thrown.");
				exception.printStackTrace();
				System.out.println("Retrying after a short delay.");
				Thread.sleep(100);
				kinesis.putRecord(putRecReq);
			} catch (ProvisionedThroughputExceededException e) {
				e.printStackTrace();
				System.out.println("Kinesis Put operation failed after re-try due to second consecutive "
						+ "ProvisionedThroughputExceededException");
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Exception thrown while writing a record to Kinesis.");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while writing a record to Kinesis.");
		}
	}

	/**
	 * This method demonstrates writing multiple messages to Kinesis Data Stream
	 * using PutRecords API.
	 * 
	 * Explicit Hash Keys: Hash Keys belong to Shards are used to write records.
	 * Partition key is needed and it can be an empty string. When both Partition
	 * Key and explicit Hash Key are set, explicit Hash Key takes precedence.
	 * Calling hashKeyIterator.next() provides a Hash Key belongs to a shard.
	 * 
	 * Retry logic: PutRecords is not atomic so it can partially reject some
	 * records. Unlike PutRecord, PutRecords does not thrown an exception rather it
	 * rejects records that are failed to write. Records are rejected for two
	 * reasons: 1. ProvisionedThroughputExceededException 2. InternalFailure. The
	 * retry logic used here handles both these errors.
	 * 
	 * Input records used for demo purpose: List of input messages is broken into
	 * smaller list objects with 500 records each. Kinesis Tip 1: Each PutRecords
	 * request can support up to 500 records. Kinesis Tip 2: Each record in the
	 * request can be as large as 1 MiB, up to a limit of 5 MiB for the entire
	 * request, including partition keys.
	 *
	 * @param recordList
	 * @param streamName
	 * @param kinesis
	 * @param hashKeyIterator
	 */
	public static void writeMessagesToKinesis(List<String> recordList, String streamName, AmazonKinesis kinesis,
			Iterator<String> hashKeyIterator) {
		PutRecordsRequest putRecsReq = new PutRecordsRequest();
		List<PutRecordsRequestEntry> putRecReqEntryList = new ArrayList<PutRecordsRequestEntry>();
		PutRecordsResult putRecsRes = new PutRecordsResult();
		List<List<String>> listofSmallerLists = Lists.partition(recordList, 500);
		for (List<String> smallerList : listofSmallerLists) {
			putRecReqEntryList.clear();
			for (String message : smallerList) {
				PutRecordsRequestEntry putRecsReqEntry = new PutRecordsRequestEntry();
				putRecsReqEntry.setData(ByteBuffer.wrap(message.getBytes()));
				putRecsReqEntry.setPartitionKey("reqiredButHasNoEffect-when-setExplicitHashKey-isUsed");
				putRecsReqEntry.setExplicitHashKey(hashKeyIterator.next());
				putRecReqEntryList.add(putRecsReqEntry);
			}
			try {
				putRecsReq.setStreamName(streamName);
				putRecsReq.setRecords(putRecReqEntryList);
				putRecsRes = kinesis.putRecords(putRecsReq);
				while (putRecsRes.getFailedRecordCount() > 0) {
					System.out.println("Processing rejected records");
					// TODO: For simplicity, the backoff implemented as a constant 100ms sleep
					// For production-grade, consider using CoralRetry's Exponential Jittered Backoff retry strategy
					// Ref: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
					Thread.sleep(100);
					final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<PutRecordsRequestEntry>();
					final List<PutRecordsResultEntry> putRecsResEntryList = putRecsRes.getRecords();
					for (int i = 0; i < putRecsResEntryList.size(); i++) {
						final PutRecordsRequestEntry putRecordReqEntry = putRecReqEntryList.get(i);
						final PutRecordsResultEntry putRecordsResEntry = putRecsResEntryList.get(i);
						if (putRecordsResEntry.getErrorCode() != null) {
							failedRecordsList.add(putRecordReqEntry);
						}
					}
					putRecReqEntryList = failedRecordsList;
					putRecsReq.setRecords(putRecReqEntryList);
					putRecsRes = kinesis.putRecords(putRecsReq);
				} // end of while loop
			} catch (Exception e) {
				System.out.println("Exception in Kinesis Batch Insert: " + e.getMessage());
			}
		}
	}

}
