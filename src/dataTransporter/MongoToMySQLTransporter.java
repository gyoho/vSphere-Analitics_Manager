package dataTransporter;


import com.mongodb.AggregationOutput;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientURI;

import java.net.UnknownHostException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TimeZone;

/** @Tip
 * 
 *  After finishing the operations close connection
 *  The object itself goes out of scope
 *  BUT, the session is still unless explicitely close it
 *
 */

public class MongoToMySQLTransporter {
	
	private static ArrayList<BasicDBObject> documentList;
	private static ArrayList<BasicDBObject> documentListHourly;
	private static String TABLE_NAME_VMLOGS = "vmLogs";
	private static String TABLE_NAME_VMLOGS_HOURLY = "vmLogsHourly";
	
	public static void transportData() throws ClassNotFoundException, SQLException, ParseException, UnknownHostException {
		documentList =  MongoDBConnector.getcleanedCollection();
		MySQLConnector.insertLogs(documentList, TABLE_NAME_VMLOGS);
	}
	
	public static void transportHourlyData() throws ClassNotFoundException, SQLException, ParseException, UnknownHostException {
		documentListHourly =  MongoDBConnector.getHourlyCollection();
		MySQLConnector.insertHourlyLogs(documentListHourly, TABLE_NAME_VMLOGS_HOURLY);
	}
	
	
	private static class MongoDBConnector {
	
		static final String DB_NAME = "cmpe283_project2";
		static final String ORI_COLLECTION_NAME = "vmLogs";
		static final String AGG_COLLECTION_NAME = "vmLogsHourly";
		
		public static ArrayList<BasicDBObject> getcleanedCollection() throws UnknownHostException {
			
			// Standard URI format: mongodb://[dbuser:dbpassword@]host:port/dbname 
	        MongoClientURI uri = new MongoClientURI("mongodb://gyoho:team6@ds053310.mongolab.com:53310/cmpe283_project2"); 
	        MongoClient client = new MongoClient(uri);
	        DB db = client.getDB(DB_NAME);
	        System.out.println("Mongo: connected to the remote database " + DB_NAME);
			 
			// 1. get a collection
	        DBCollection oriCol = db.getCollection(ORI_COLLECTION_NAME);
	        System.out.println("Mongo: collection " + ORI_COLLECTION_NAME + " selected");
			
	        
	        // 2. clean the data
	        Iterable<DBObject> cleanCol = cleanData(oriCol).results();
	        System.out.println("Mongo: cleaned the logs in the selected collection");
	        
	        /*for (DBObject result : cleanCol) {
	            System.out.println(result);
	        }*/
	        
	        
	        // 3. copy the data to the list
	        ArrayList<BasicDBObject> docList = new ArrayList<BasicDBObject>();
			for (DBObject result : cleanCol) {
				docList.add((BasicDBObject) result);
	        }
	        
	        
	        // 4. store the processed data into the hourly collection
			// 4.1. get a collection
	        DBCollection aggCol = db.getCollection(AGG_COLLECTION_NAME);
	        System.out.println("Mongo: inserting to the cleaned logs into " + AGG_COLLECTION_NAME);
	        
	        // 4.2 insert the cleaned collection into the new collection
	        for (BasicDBObject doc : docList) {
	        	aggCol.insert(doc);
	        }
            System.out.println("Mongo: document inserted successfully");
			
	        
	        // 5. delete the original data
	        // c.f. col.remove({})
            oriCol.remove(new BasicDBObject());
	        System.out.println("Mongo: deleted all records");
	        
	        
	        /** close the connection, otherwise too many threads accessing the db
	         *  MongoLab has limitation for concurrent accesses **/
	        client.close();
	        
	        // 6. return the processed data for MySQL to use
	        return docList;
		}
		
		public static ArrayList<BasicDBObject> getHourlyCollection() throws UnknownHostException {
			
			// Standard URI format: mongodb://[dbuser:dbpassword@]host:port/dbname 
	        MongoClientURI uri = new MongoClientURI("mongodb://gyoho:team6@ds053310.mongolab.com:53310/cmpe283_project2"); 
	        MongoClient client = new MongoClient(uri);
	        DB db = client.getDB(DB_NAME);
	        System.out.println("Mongo: connected to the remote database " + DB_NAME);
			 
			// 1. get a collection
	        DBCollection aggCol = db.getCollection(AGG_COLLECTION_NAME);
	        System.out.println("Mongo: collection " + AGG_COLLECTION_NAME + " selected");
	        
	        // 2. aggregate the data
	        Iterable<DBObject> hourlyCol = calculateHourlyData(aggCol).results();
	        System.out.println("Mongo: aggregated the logs by hour");
	        /*for (DBObject result : hourlyCol) {
	            System.out.println(result);
	        }*/
	        
	        // 3. copy the data to the list
	        ArrayList<BasicDBObject> docList = new ArrayList<BasicDBObject>();
			for (DBObject result : hourlyCol) {
				docList.add((BasicDBObject) result);
	        }
	        
	        // 4. delete the original data
	        aggCol.remove(new BasicDBObject());
	        System.out.println("Mongo: deleted all records in " + AGG_COLLECTION_NAME);
	        
	        /** close the connection, otherwise too many threads accessing the db
	         *  MongoLab has limitation for concurrent accesses **/

	        client.close();
	        
	        // 5. return the processed data for MySQL to use
	        return docList;
		}
		
		private static AggregationOutput cleanData(DBCollection col) {
			// build the 1st pipeline operations: get milliseconds
			DBObject fields1 = new BasicDBObject("_id", 0);
			fields1.put("@timestamp", 1);
			fields1.put("vmType", 1);
			fields1.put("vmName", 1);
			fields1.put("groupInfo", 1);
			fields1.put("nameInfo", 1);
			fields1.put("rollupType", 1);
			fields1.put("unitInfo", 1);
			fields1.put("value", 1);
			fields1.put("ml", new BasicDBObject( "$millisecond", "$@timestamp"));
			DBObject project1 = new BasicDBObject("$project", fields1 );
			

			// build the 2nd projection operation: subtract milliseconds
			DBObject fields2 = new BasicDBObject("@timestamp", 1);
			fields2.put("vmType", 1);
			fields2.put("vmName", 1);
			fields2.put("groupInfo", 1);
			fields2.put("nameInfo", 1);
			fields2.put("rollupType", 1);
			fields2.put("unitInfo", 1);
			fields2.put("value", 1);
			fields2.put("timestamp", new BasicDBObject( "$subtract", Arrays.<Object> asList("$@timestamp", "$ml")));
			DBObject project2 = new BasicDBObject("$project", fields2 );

			// Now the $group operation
			DBObject idGroupFields = new BasicDBObject( "@timestamp", "$timestamp");
			idGroupFields.put("vmName", "$vmName");
			idGroupFields.put("groupInfo", "$groupInfo");
			idGroupFields.put("vmType", "$vmType" );
			idGroupFields.put("nameInfo", "$nameInfo");
			idGroupFields.put("rollupType", "$rollupType" );
			idGroupFields.put("unitInfo", "$unitInfo" );
			
			DBObject groupFields = new BasicDBObject( "_id", idGroupFields);
			groupFields.put("value", new BasicDBObject( "$avg", "$value"));
			DBObject group = new BasicDBObject("$group", groupFields);

			
			// build the last projection operation: subtract milliseconds
			DBObject fields4 = new BasicDBObject("_id", 0);
			fields4.put("@timestamp", "$_id.@timestamp");
			fields4.put("vmType", "$_id.vmType");
			fields4.put("vmName", "$_id.vmName");
			fields4.put("groupInfo", "$_id.groupInfo");
			fields4.put("nameInfo", "$_id.nameInfo");
			fields4.put("rollupType", "$_id.rollupType");
			fields4.put("unitInfo", "$_id.unitInfo");
			fields4.put("value", 1);
			DBObject project4 = new BasicDBObject("$project", fields4 );

			// run aggregation
			AggregationOutput output = col.aggregate(project1, project2, group, project4);
			return output;
		}
		
		private static AggregationOutput calculateHourlyData(DBCollection col) {
			
			// $group operation
			DBObject idGroupFields = new BasicDBObject("vmName", "$vmName");
			idGroupFields.put("groupInfo", "$groupInfo");
			idGroupFields.put("vmType", "$vmType" );
			idGroupFields.put("nameInfo", "$nameInfo");
			idGroupFields.put("rollupType", "$rollupType" );
			idGroupFields.put("unitInfo", "$unitInfo" );
			
			DBObject groupFields = new BasicDBObject( "_id", idGroupFields);
			groupFields.put("value", new BasicDBObject( "$avg", "$value"));
			DBObject group = new BasicDBObject("$group", groupFields);
			
			
			// build the last projection operation: subtract milliseconds
			DBObject fields = new BasicDBObject("_id", 0);
			fields.put("vmType", "$_id.vmType");
			fields.put("vmName", "$_id.vmName");
			fields.put("groupInfo", "$_id.groupInfo");
			fields.put("nameInfo", "$_id.nameInfo");
			fields.put("rollupType", "$_id.rollupType");
			fields.put("unitInfo", "$_id.unitInfo");
			fields.put("value", 1);
			DBObject project = new BasicDBObject("$project", fields );
			
			// run aggregation
			AggregationOutput output = col.aggregate(group, project);
			return output;
		}
	}
	
	private static class MySQLConnector {
		
		/** ClearDB **/
		// JDBC driver name and database URL
		static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		static final String DB_URL = "jdbc:mysql://us-cdbr-iron-east-01.cleardb.net/ad_4a646f70a83ab03";
		
		//  Database credentials
		static final String USER = "b84cf6af4ff1fa";
		static final String PASS = "6edb6b8d";
		
		public static void insertLogs(ArrayList<BasicDBObject> docList, String TABLE_NAME) throws SQLException, ParseException, ClassNotFoundException {
			
			Connection conn = null;
			
			// Register JDBC driver
			Class.forName(JDBC_DRIVER);
			
			// Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("MySQL: connected database successfully...");
		
			
			/** Execute Statement **/
			Statement stmt = null;
		    stmt = conn.createStatement();
		    

		    /** reference **/
//		    INSERT INTO table_name (column1,column2,column3,...)
//		    VALUES (value1,value2,value3,...);
		    
		    for(BasicDBObject doc : docList) {
		    	
		    	Timestamp timestamp = parseDateISO(doc.getString("@timestamp"));
		    	String vmType = doc.getString("vmType");
		    	String vmName = doc.getString("vmName");
		    	String groupInfo = doc.getString("groupInfo");
		    	String nameInfo = doc.getString("nameInfo");
		    	String rollupType = doc.getString("rollupType");
		    	String unitInfo = doc.getString("unitInfo");
		    	Integer value = doc.getInt("value");
		    	
		    	
			    String sql = "INSERT IGNORE INTO " + TABLE_NAME
			    		+ " (timestamp, vmType, vmName, groupInfo, nameInfo, rollupType, unitInfo, value)"
			    		+ " VALUES('" + timestamp + "', '" + vmType	+ "', '" + vmName + "', '" + groupInfo 
			    		+ "', '" + nameInfo + "', '" + rollupType + "', '" + unitInfo + "', " + value + ")";

			    
			    stmt.executeUpdate(sql);
		    }
		    
		    // Execute a query
		    System.out.println("MySQL: inserted records into the table " + TABLE_NAME);
		    
		    /** close the connection, otherwise too many threads accessing the db
	         *  ClearDB has limitation for concurrent accesses **/
		    stmt.close();
		    conn.close();
		}
		
		/*
		 * format of time zone in mongodb:
		 * "@timestamp" : "\"2014-11-24T05:24:31.484Z\""
		 * "@timestamp" : ISODate("2014-12-04T08:13:00.219Z")				
		 */
		private static Timestamp parseDateISO(String timestamp) throws ParseException {
			SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
			formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
			java.util.Date date = formatter.parse(timestamp);
			Timestamp sqltimestamp = new Timestamp(date.getTime());
			
			return sqltimestamp;
		}
		
		
		public static void insertHourlyLogs(ArrayList<BasicDBObject> docList, String TABLE_NAME) throws SQLException, ParseException, ClassNotFoundException {
			
			Connection conn = null;
			
			// Register JDBC driver
			Class.forName(JDBC_DRIVER);
			
			// Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("MySQL: connected database successfully...");
		
			
			/** Execute Statement **/
			Statement stmt = null;
		    stmt = conn.createStatement();
		    

		    /** reference **/
//		    INSERT INTO table_name (column1,column2,column3,...)
//		    VALUES (value1,value2,value3,...);
		    
		    for(BasicDBObject doc : docList) {
		    	
//		    	String id = doc.getString("_id");
		    	long time = System.currentTimeMillis();
		    	Timestamp timestamp = new Timestamp(time);
		    	String vmType = doc.getString("vmType");
		    	String vmName = doc.getString("vmName");
		    	String groupInfo = doc.getString("groupInfo");
		    	String nameInfo = doc.getString("nameInfo");
		    	String rollupType = doc.getString("rollupType");
		    	String unitInfo = doc.getString("unitInfo");
		    	Integer value = doc.getInt("value");
		    	
		    	
			    String sql = "INSERT IGNORE INTO " + TABLE_NAME
			    		+ " (timestamp, vmType, vmName, groupInfo, nameInfo, rollupType, unitInfo, value)"
			    		+ " VALUES('" + timestamp + "', '" + vmType	+ "', '" + vmName + "', '" + groupInfo 
			    		+ "', '" + nameInfo + "', '" + rollupType + "', '" + unitInfo + "', " + value + ")";

			    
			    stmt.executeUpdate(sql);
		    }
		    
		    // Execute a query
		    System.out.println("MySQL: inserted records into the table " + TABLE_NAME);
		    
		    /** close the connection, otherwise too many threads accessing the db
	         *  ClearDB has limitation for concurrent accesses **/
		    stmt.close();
		    conn.close();
		}
	}
}