package com.amazonaws.dynamo;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public class DynamoUtil {
	public static ItemCollection<QueryOutcome> queryTable(DynamoDB dynamoDB, QuerySpec spec, String tableName) {
		Table table = dynamoDB.getTable(tableName);
		ItemCollection<QueryOutcome> items = table.query(spec);
		return items;
	}
}
