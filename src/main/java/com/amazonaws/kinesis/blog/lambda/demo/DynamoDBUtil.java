// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.kinesis.blog.lambda.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.collect.Lists;

/**
 * <p>
 * This is a utility class with methods to read / write Kinesis Shard details
 * from / to a DynamoDB table.
 * <p>
 * 
 * @author Ravi Itha, Amazon Web Service, Inc.
 *
 */
public class DynamoDBUtil {

	/**
	 * Method to write Shard details to a DynamoDB table.
	 * 
	 * @param openShards
	 * @param dynamoDBTblName
	 */
	public void insertHashkeysToDynamoDB(List<KinesisShard> openShards, String dynamoDBTblName) {

		AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().build();
		List<WriteRequest> itemList = new ArrayList<WriteRequest>();
		for (KinesisShard shard : openShards) {
			Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
			item.put("shard_id",
					new AttributeValue().withS(shard.getShardId()));
			item.put("stream_name", new AttributeValue().withS(shard.getStreamName()));
			item.put("starting_hash_key", new AttributeValue().withS(shard.getStartingHashKey()));
			item.put("ending_hash_key", new AttributeValue().withS(shard.getEndingHashKey()));
			itemList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item)));
		}
		for (List<WriteRequest> miniBatch : Lists.partition(itemList, 25)) {
			Map<String, List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
			requestItems.put(dynamoDBTblName, miniBatch);
			BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest()
					.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL).withRequestItems(requestItems);
			try {
				BatchWriteItemResult result = dynamoDB.batchWriteItem(batchWriteItemRequest);
				while (result.getUnprocessedItems().size() > 0) {
					Map<String, List<WriteRequest>> unprocessedItems = result.getUnprocessedItems();
					result = dynamoDB.batchWriteItem(unprocessedItems);
				}
			} catch (AmazonDynamoDBException e) {
				e.printStackTrace();
				System.out.println("Could not insert into DynamoDB");
			}
		}
		System.out.println("Shard details are inserted to DynamoDB table: " + dynamoDBTblName);
		dynamoDB.shutdown();
	}

	/**
	 * Method to read Shard details from a DynamoDB table.
	 * 
	 * @param tableName
	 * @param streamName
	 * @return
	 */
	public List<String> getHashkeys(String tableName, String streamName) {
		System.out.println("Scanning Starting Hashkeys for Stream: " + streamName);
		List<String> hashKeyListForOpenShards = Lists.newArrayList();
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
		eav.put(":val1", new AttributeValue().withS(streamName));
		int scanCount = 0;
		do {
			ScanRequest scanRequest = new ScanRequest().withTableName(tableName).withLimit(100)
					.withFilterExpression("stream_name = :val1").withExpressionAttributeValues(eav)
					.withExclusiveStartKey(lastKeyEvaluated);
			ScanResult result = client.scan(scanRequest);
			List<Map<String, AttributeValue>> subList = result.getItems();
			for (Map<String, AttributeValue> item : subList) {
				String startingHashKey = item.get("starting_hash_key").getS().toString();
				hashKeyListForOpenShards.add(startingHashKey);
			}
			lastKeyEvaluated = result.getLastEvaluatedKey();
			scanCount++;
		} while (lastKeyEvaluated != null);
		System.out.println("Number of times table has scanned: " + scanCount);
		System.out.println("Number of hashkeys found: " + hashKeyListForOpenShards.size());
		return hashKeyListForOpenShards;
	}

}