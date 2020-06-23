// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.kinesis.blog.lambda.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

//import com.amazonaws.kinesis.blog.demo.KDSUtil;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * <p>
 * Sample Lambda function to read data from an S3 Object and write to Kinesis
 * Stream.
 * <p>
 * 
 * @author Ravi Itha, Amazon Web Service, Inc.
 *
 */
public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

	public LambdaFunctionHandler() {
	}

	// Test purpose only.
	LambdaFunctionHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	/**
	 * This the main Handler method. It process records from an Object in S3 bucket 
	 * and write those records to a target Kinesis Stream.
	 */
	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);
		String contentType = "";
		String ddbTblName4HashKeys = Optional.ofNullable(System.getenv("tbl_kinesis_shard_hashkeys"))
				.orElse("kinesis_hash_keys");
		String targetKinesiStream = Optional.ofNullable(System.getenv("target_kinesis_stream"))
				.orElse("stream_with_100_shards");
		String region = Optional.ofNullable(System.getenv("region")).orElse("us-east-1");
		S3Object fullObject = null;
		DynamoDBUtil ddbUtil = new DynamoDBUtil();
		AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();

		// Option 1: get Hash Keys from a pre-populated DynamoDB table
		List<String> hashKeyListForOpenShards = ddbUtil.getHashkeys(ddbTblName4HashKeys, targetKinesiStream);
		
		// Option 2: get Hash Keys directly Kinesis Stream. Use this option when Option 1 is not used.
		// KDSUtil kdsUtil = new KDSUtil();
		// List<String> hashKeyListForOpenShards = kdsUtil.getHashKeysForOpenShards(kinesis, targetKinesiStream);
		
		Iterator<String> hashKeyIterator = createRoundrobinListofHashKyes(hashKeyListForOpenShards);

		// Get the object from the event and show its content type
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();
		try {
			fullObject = s3.getObject(new GetObjectRequest(bucket, key));
			contentType = fullObject.getObjectMetadata().getContentType();
			context.getLogger().log("CONTENT TYPE: " + contentType);
			processRecordsFromObject(fullObject.getObjectContent(), kinesis, targetKinesiStream, hashKeyIterator);
		} catch (Exception e) {
			e.printStackTrace();
			context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and"
					+ " your bucket is in the same region as this function.", key, bucket));
		}
		return contentType;
	}

	private void processRecordsFromObject(InputStream input, AmazonKinesis kinesis, String targetKinesiStream,
			Iterator<String> hashKeyIterator) throws IOException {
		// Read the text input stream one line at a time and display each line.
		List<String> recordList = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line = null;
		while ((line = reader.readLine()) != null) {
			// System.out.println(line);
			recordList.add(line);
			if (recordList.size() == 500) {
				writeMessagesToKinesis(recordList, targetKinesiStream, kinesis, hashKeyIterator);
				recordList.clear();
			}
		}
		if (recordList.size() > 0) {
			writeMessagesToKinesis(recordList, targetKinesiStream, kinesis, hashKeyIterator);
			recordList.clear();
		}

		System.out.println();
	}

	/**
	 * This method demonstrates writing multiple messages to Kinesis Data Stream
	 * using PutRecords API.
	 * 
	 * Explicit Hash Keys: Hash Keys belong to shards are used to write records.
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
	 * @param msgList
	 * @param streamName
	 * @param kinesis
	 * @param hashKeyIterator
	 */
	public void writeMessagesToKinesis(List<String> msgList, String streamName, AmazonKinesis kinesis,
			Iterator<String> hashKeyIterator) {
		PutRecordsRequest putRecsReq = new PutRecordsRequest();
		List<PutRecordsRequestEntry> putRecReqEntryList = new ArrayList<PutRecordsRequestEntry>();
		PutRecordsResult putRecsRes = new PutRecordsResult();
		List<List<String>> listofSmallerLists = Lists.partition(msgList, 500);
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
				}
				System.out.printf("%d records inserted to Kinesis Stream successfully.\n", smallerList.size());
			} catch (Exception e) {
				System.out.println("Exception in Kinesis Batch Insert: " + e.getMessage());
			}
		}
	}

	/**
	 * This method creates a circular list based on a standard array list.
	 * 
	 * @param hashKeysList
	 * @return
	 */
	public Iterator<String> createRoundrobinListofHashKyes(List<String> hashKeysList) {
		Iterator<String> hashKeyIterator = Iterables.cycle(hashKeysList).iterator();
		return hashKeyIterator;
	}
}