package com.amazonaws.lambda.google;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.json.JSONArray;

import com.amazonaws.dynamo.ConfigUtil;
import com.amazonaws.lambda.google.util.GooglePlacesUtil;
import com.amazonaws.lambda.vo.PlaceVO;
import com.amazonaws.postgre.PostgreSavePlaces;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GooglePlacesApiHandler implements RequestHandler<Object, String> {

	@Override
	public String handleRequest(Object input, Context context) {
		context.getLogger().log("Input: " + input.getClass());
		
		ObjectMapper mapper = new ObjectMapper();
		String inputJson;
		try {
			inputJson = mapper.writeValueAsString(input);
		} catch (JsonProcessingException e) {
			return "Something went wrong, unable to parse input : " + e;
		}
		
		AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient();
		dynamoDBClient.setRegion(Region.getRegion(Regions.US_EAST_2));
		DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
		
		String googlePlacesApiKey = ConfigUtil.getConfigValue(dynamoDB, "googlePlacesKey");
		
		List<PlaceVO> places = null;
		try {
			places = GooglePlacesUtil.processInput(inputJson, googlePlacesApiKey);
		} catch (Exception e1) {
			return "Something went wrong : " + e1;
		}

		try {
			PostgreSavePlaces.save(dynamoDB, places);
		} catch (ClassNotFoundException | SQLException e) {
			return "Something went wrong whle saving places in postgreSql";
		}
		
		Set<String> placesSet = new HashSet<String>();
		for(int i =0 ; i< places.size(); i++) {
			PlaceVO place = places.get(i);
			placesSet.add(place.getName());
		}
		
		return new JSONArray(placesSet).toString();
	}

}
