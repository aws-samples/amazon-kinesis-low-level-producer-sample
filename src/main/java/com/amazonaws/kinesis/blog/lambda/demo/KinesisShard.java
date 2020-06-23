// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.kinesis.blog.lambda.demo;

/**
 * <p>
 * This is a POJO class for Kinesis Shard. It hold details for a shard.
 * <p>
 * 
 * @author Ravi Itha, Amazon Web Service, Inc.
 *
 */
public class KinesisShard {
	private String streamName;
	private String shardId;
	private String startingHashKey;
	private String endingHashKey;

	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public String getShardId() {
		return shardId;
	}

	public void setShardId(String shardId) {
		this.shardId = shardId;
	}

	public String getStartingHashKey() {
		return startingHashKey;
	}

	public void setStartingHashKey(String startingHashKey) {
		this.startingHashKey = startingHashKey;
	}

	public String getEndingHashKey() {
		return endingHashKey;
	}

	public void setEndingHashKey(String endingHashKey) {
		this.endingHashKey = endingHashKey;
	}

}
