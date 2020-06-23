// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.kinesis.blog.lambda.demo;

import java.util.List;

import com.amazonaws.kinesis.blog.demo.KinesisStreamUtil;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

/**
 * <p>
 * This class has a method to fetch Kinesis Shard details from a stream and
 * populate those details to a DynamoDB table.
 * <p>
 * 
 * @author Ravi Itha, Amazon Web Service, Inc.
 *
 */
public class HashKeysBootstrapper {

	/**
	 * Method to fetch Shard details of a Kinesis Stream and populate a DynamoDB
	 * table.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		String region = "us-east-1";
		String streamName = "stream_with_125_shards";
		String dynamoDBTblName = "kinesis_hash_keys";
		KinesisStreamUtil kdsUtil = new KinesisStreamUtil();
		DynamoDBUtil ddbUtil = new DynamoDBUtil();
		AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();
		List<KinesisShard> openShards = kdsUtil.getOpenShardDetails(kinesis, streamName);
		System.out.printf("Kinesis Stream %s has %d shards. \n", streamName, openShards.size());
		ddbUtil.insertHashkeysToDynamoDB(openShards, dynamoDBTblName);
	}

}
