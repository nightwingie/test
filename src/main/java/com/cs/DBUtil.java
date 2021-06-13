package com.cs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

//import org.apache.log4j.Logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DBUtil {
//	final static Logger logger = Logger.getLogger(DBUtil.class);
	private static final Logger logger = LoggerFactory.getLogger(DBUtil.class);

	private static final Gson GSON = new GsonBuilder().create();

	public DBUtil() {
		try {
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void writeToDB() {
		logger.debug("This is debug");
		try {
			Connection c = DriverManager.getConnection("jdbc:hsqldb:file:testDB", "SA", "");
			PreparedStatement ps = c.prepareStatement("INSERT INTO CS.LOG_EVENTS (ID, DURATION, HOST_TYPE, ALERT) VALUES ( ?, ?, ?, ?)");
			ps.setString(1, "4567");
			ps.setInt(2, 5);
			ps.setString(3, "APPLICATION_LOG");
			ps.setBoolean(4, true);
			int count = ps.executeUpdate();
			System.out.println("updated: " + count);
			c.commit();
			c.close();
		} catch (SQLException e) {
			logger.error("failed to save to DB: ", e);
		}

		Map<String, Integer> tem = new HashMap<>();
		tem.put("test", 123);
		tem.put("test2", 456);

		System.out.println(GSON.toJson(tem));
	}

	public static void main(String[] args) {
		new DBUtil().writeToDB();
	}
}
