package com.amazonaws.postgre;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class PostgreUtil {
	public static ResultSet executeSelect(DynamoDB dynamoDB, String sql) throws ClassNotFoundException, SQLException {
		Connection connection = PostgreConnectionFactory.getConnection(dynamoDB);
		Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        return resultSet;
	}
}
