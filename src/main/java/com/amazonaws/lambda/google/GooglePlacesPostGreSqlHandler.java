package com.amazonaws.lambda.google;

import java.sql.SQLException;
import java.util.List;

import com.amazonaws.lambda.vo.PlaceVO;
import com.amazonaws.postgre.PostgreReadPlaces;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GooglePlacesPostGreSqlHandler implements RequestHandler<Object, String> {

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
		
		List<PlaceVO> resultPlaces = null;
		try {
			resultPlaces = PostgreReadPlaces.processRequest(dynamoDB, inputJson);
		} catch (ClassNotFoundException | SQLException e) {
			return "Something went wrong : " + e;
		}
		
		String outputJson = null;
		try {
			outputJson = mapper.writeValueAsString(resultPlaces);
		} catch (JsonProcessingException e) {
			return "Something went wrong, unable to parse output : " + e;
		}
		
		return outputJson;
	}

}
