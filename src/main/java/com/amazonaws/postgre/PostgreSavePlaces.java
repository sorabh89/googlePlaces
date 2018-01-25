package com.amazonaws.postgre;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import com.amazonaws.lambda.vo.PlaceVO;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class PostgreSavePlaces {

	static String insertSql = "insert into placesInfo (latitude, longitude, name, type) values(?,?,?,?)";
	
	public static void save(DynamoDB dynamoDB, List<PlaceVO> places) throws ClassNotFoundException, SQLException {
		Connection connection = PostgreConnectionFactory.getConnection(dynamoDB);
		
		PreparedStatement statement = connection.prepareStatement(insertSql);
		
		for(int i =0 ; i< places.size(); i++) {
			PlaceVO place = places.get(i);
			statement.setDouble(1,place.getLatitude());
			statement.setDouble(2,place.getLongitude());
			statement.setString(3,place.getName());
			statement.setString(4,place.getType());  
			statement.executeUpdate();  
		}
		
		PostgreConnectionFactory.closeConnection();
	}

}
