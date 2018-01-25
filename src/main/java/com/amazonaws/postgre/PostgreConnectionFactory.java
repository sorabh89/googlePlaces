package com.amazonaws.postgre;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.amazonaws.dynamo.ConfigUtil;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class PostgreConnectionFactory {
	
	static Connection connection = null;
	
	public static Connection getConnection(DynamoDB dynamoDB) throws ClassNotFoundException, SQLException {
		String postgreSqlHostName = ConfigUtil.getConfigValue(dynamoDB, "postgreSqlHostName");
		String postgreSqlPort = ConfigUtil.getConfigValue(dynamoDB, "postgreSqlPort");
		String postgreSqlDBName = ConfigUtil.getConfigValue(dynamoDB, "postgreSqlDBName");
		String postgreSqlUser = ConfigUtil.getConfigValue(dynamoDB, "postgreSqlUser");
		String postgreSqlPass = ConfigUtil.getConfigValue(dynamoDB, "postgreSqlPass");
		
		String dbURL = "jdbc:postgresql://"+postgreSqlHostName+":"+postgreSqlPort+"/"+postgreSqlDBName; 
		
		Class.forName("org.postgresql.Driver");
		Properties props = new Properties();
		props.setProperty("user", postgreSqlUser);
		props.setProperty("password", postgreSqlPass);
		connection = DriverManager.getConnection(dbURL, props);
		return connection;
	}
	
	public static void closeConnection() throws SQLException {
		if(connection != null) {
			connection.close();
		}
	}
	
}
