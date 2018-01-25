package com.amazonaws.postgre;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.lambda.vo.PlaceVO;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.grum.geocalc.Coordinate;
import com.grum.geocalc.DegreeCoordinate;
import com.grum.geocalc.EarthCalc;
import com.grum.geocalc.Point;

public class PostgreReadPlaces {

	static double earthRadius = 6371;
	static String query = "select distinct latitude, longitude, name, type from placesInfo where latitude <= %f and latitude >= %f and longitude <= %f and longitude >= %f and type in (%s)";
	
	public static List<PlaceVO> processRequest(DynamoDB dynamoDB, String inputJson) throws ClassNotFoundException, SQLException {
		JSONObject jsonObject  = new JSONObject(inputJson);
		
		double latitude = jsonObject.getDouble("latitude");
		double longitude = jsonObject.getDouble("longitude");
		double radius = jsonObject.getDouble("radius");
		List<String> places = new ArrayList<String>();
		JSONArray types = jsonObject.getJSONArray("types");
		for(int j = 0; j < types.length(); j++)
		{
			places.add("'"+types.getString(j)+"'");
		}
		
		String sql = findExtremePointsAndFormatSQL(latitude, longitude, radius, places);
		ResultSet resultSet = PostgreUtil.executeSelect(dynamoDB, sql);
		List<PlaceVO> possiblePlacesList = getPlacesList(resultSet);
		List<PlaceVO> outputPlacesList = filterOutsidePlaces(latitude, longitude, radius, possiblePlacesList);
		return outputPlacesList;
	}

	private static List<PlaceVO> filterOutsidePlaces(double latitude, double longitude, double radius, List<PlaceVO> possiblePlacesList) {
		Point centerPoint = new Point(new DegreeCoordinate(latitude), new DegreeCoordinate(longitude));
		List<PlaceVO> outputPlacesList = new ArrayList<PlaceVO>();
		for(PlaceVO vo : possiblePlacesList) {
			Point point = new Point(new DegreeCoordinate(vo.getLatitude()), new DegreeCoordinate(vo.getLongitude()));
			double distance = EarthCalc.getDistance(centerPoint, point);
			if(distance<=radius) {
				outputPlacesList.add(vo);
			}
		}
		return outputPlacesList;
	}

	private static List<PlaceVO> getPlacesList(ResultSet resultSet) throws SQLException {
		List<PlaceVO> possiblePlacesList = new ArrayList<PlaceVO>();
		while (resultSet.next()) {
			PlaceVO vo = new PlaceVO();
            vo.setLatitude(resultSet.getDouble("latitude"));
            vo.setLongitude(resultSet.getDouble("longitude"));
            vo.setName(resultSet.getString("name"));
            vo.setType(resultSet.getString("type"));
            possiblePlacesList.add(vo);
        }
		return possiblePlacesList;
	}

	private static String findExtremePointsAndFormatSQL(double latitude, double longitude, double radius, List<String> places) {
		Coordinate lat = new DegreeCoordinate(latitude);
		Coordinate lng = new DegreeCoordinate(longitude);
		Point kew = new Point(lat, lng);

		Point pointE = EarthCalc.pointRadialDistance(kew, 90, radius);
		Point pointW = EarthCalc.pointRadialDistance(kew, 270, radius);
		Point pointN = EarthCalc.pointRadialDistance(kew, 0, radius);
		Point pointS = EarthCalc.pointRadialDistance(kew, 180, radius);
		
		String sql = String.format(query, pointN.getLatitude(), pointS.getLatitude(), pointE.getLongitude(), pointW.getLongitude(), String.join(",", places));
		
		return sql;
	}

}
