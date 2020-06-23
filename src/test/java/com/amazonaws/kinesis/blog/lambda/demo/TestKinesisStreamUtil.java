// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.kinesis.blog.lambda.demo;

import java.util.List;

import com.amazonaws.kinesis.blog.demo.KinesisStreamUtil;
import com.amazonaws.kinesis.blog.lambda.demo.KinesisShard;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

/**
 * Test class to test methods from KDSUtil class
 * 
 * @author Ravi Itha, Amazon Web Service, Inc. 
 *
 */
public class TestKinesisStreamUtil {

	public static void main(String[] args) {
		String region = "us-east-1";
		String streamName = "stream_with_200_shards";
		AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();
		KinesisStreamUtil kdsUtil = new KinesisStreamUtil();

		System.out.println("Fetch Starting Hash Key for all open shards of Kinesis Stream: " + streamName);
		List<String> hashKeyListForOpenShards = kdsUtil.getHashKeysForOpenShards(kinesis, streamName);
		for (String stringHashKey : hashKeyListForOpenShards) {
			System.out.println("Starting Hash Key: " + stringHashKey);
		}

		System.out.println("Fetch open shard details of Kinesis Stream: " + streamName);
		List<KinesisShard> openShards = kdsUtil.getOpenShardDetails(kinesis, streamName);
		for (KinesisShard kinesisShard : openShards) {
			System.out.printf("Shard Id: %s, Starting Hash Key: %s, Ending Hash Key: %s \n ", kinesisShard.getShardId(),
					kinesisShard.getStartingHashKey(), kinesisShard.getEndingHashKey());
		}

	}

}
