package com.amazonaws.dynamo;

import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class ConfigUtil {
	public static String getConfigValue(DynamoDB dynamoDB, String configKey) {
		QuerySpec spec = new QuerySpec().withKeyConditionExpression("CONFIG_KEY = :v_partitionKey").withValueMap(new ValueMap().withString(":v_partitionKey", configKey));
		ItemCollection<QueryOutcome> items = DynamoUtil.queryTable(dynamoDB, spec, "CONFIGS_UTIL");
		Iterator<Item> iterator = items.iterator();
		
		String configValue = null;
		while (iterator.hasNext()) {
		    Item item = iterator.next();
		    configValue = item.getString("CONFIG_VALUE");
		}
		return configValue;
	}
}
