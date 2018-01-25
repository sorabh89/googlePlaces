package com.amazonaws.lambda.google.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.lambda.vo.PlaceVO;
import com.amazonaws.services.lambda.runtime.Context;

public class GooglePlacesUtil {
	
	public static List<PlaceVO> processInput(String requestJson, String googlePlacesApiKey) throws Exception {
		String url = generateGooglePlacesUrl(requestJson, googlePlacesApiKey);
		String response = callGooglePlacesApi(url);
		List<PlaceVO> places = processGoogleResponse(response);
		return places;
	}

	public static String generateGooglePlacesUrl(String requestJson, String googlePlacesApiKey) {
		
		String url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=%f,%f&radius=%f&types=%s&key=%s";
		
		JSONObject jsonObject  = new JSONObject(requestJson);
		double latitude = jsonObject.getDouble("latitude");
		double longitude = jsonObject.getDouble("longitude");
		double radius = jsonObject.getDouble("radius");
		String typesStr = "";
		JSONArray types = jsonObject.getJSONArray("types");
		for(int j = 0; j < types.length(); j++)
		{
			if(typesStr.equals("")) {
				typesStr += types.getString(j);
			} else {
				typesStr += "|"+types.getString(j);
			}
		}
		
		return String.format(url, latitude, longitude, radius, typesStr, googlePlacesApiKey);
	}
	
	public static String callGooglePlacesApi(String urlStr) throws Exception {
		URL url = new URL(urlStr);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");

		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		String output = "";
		String line;
		while ((line = br.readLine()) != null) {
			output += line;
		}
		
		conn.disconnect();

		return output;
	}
	
	public static List<PlaceVO> processGoogleResponse(String response) {
		List<PlaceVO> places = new ArrayList<PlaceVO>();
		
		JSONObject jsonObject  = new JSONObject(response);
		JSONArray result = jsonObject.getJSONArray("results");
		for(int i = 0; i < result.length(); i++)
		{
		      JSONObject place = result.getJSONObject(i);
		      JSONObject location = place.getJSONObject("geometry").getJSONObject("location");
		      JSONArray types = place.getJSONArray("types");
		      double lat = location.getDouble("lat");
		      double lng = location.getDouble("lng");
		      String name = place.getString("name");
		      for(int j = 0; j < types.length(); j++)
				{
		    	  PlaceVO vo = new PlaceVO();
		    	  vo.setLatitude(lat);
		    	  vo.setLongitude(lng);
		    	  vo.setName(name);
		    	  vo.setType(types.getString(j));
		    	  places.add(vo);
				}
		}
		return places;
	}

}
