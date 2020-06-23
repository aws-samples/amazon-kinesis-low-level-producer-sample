package com.amazonaws.kinesis.blog.lambda.demo;

import java.util.Iterator;
import java.util.List;

import com.amazonaws.kinesis.blog.lambda.demo.DynamoDBUtil;
import com.google.common.collect.Iterables;

public class TestDynamoDB {

	public static void main(String[] args) {
		
		String targetKinesiStream = "stream_with_10_shards";
		String ddbTblName4HashKeys = "kinesis_hash_keys";
		DynamoDBUtil ddbUtil = new DynamoDBUtil();
		List<String> hashKeyListForOpenShards = ddbUtil.getHashkeys(ddbTblName4HashKeys, targetKinesiStream);
		for(String hashKey : hashKeyListForOpenShards) {
			System.out.println("Starting Hashkey: " + hashKey);
		}
		System.out.println("Calling round-robin algorithm");
		Iterator<String> hashKeyIterator = createRoundrobinListofHashKyes(hashKeyListForOpenShards);
		for(int i=1; i < 12 ; i++) {
			System.out.println("Hash Key - " + i + ": " + hashKeyIterator.next());
		}
	
	}
	
	public static Iterator<String> createRoundrobinListofHashKyes(List<String> hashKeysList) {
		Iterator<String> hashKeyIterator = Iterables.cycle(hashKeysList).iterator();
		return hashKeyIterator;
	}

}
