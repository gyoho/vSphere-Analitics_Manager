package components;

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;

import java.net.UnknownHostException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TimeZone;

public class MongoToMySQLTransporter {
	
	private static DBCollection collection;
	private static Connection database;
	private static ArrayList<BasicDBObject> documentList;
	
	public static void makeConnection() throws UnknownHostException, ClassNotFoundException, SQLException {
		collection = MongoDBConnector.connect();
		database = MySQLConnector.connect();
			
	}
	
	public static void transportData() throws ClassNotFoundException, SQLException, ParseException {
		documentList = MongoDBConnector.retrive(collection);
		MySQLConnector.insert(database, documentList);
	}
	
	
	private static class MongoDBConnector {
		
		static final String HOST_NAME = "localhost";
		static final Integer PORT_NUM = 27017;
		static final String DB_NAME = "cmpe283_Project2";
		static final String COLLECTION_NAME = "vmLogs";
		
		public static DBCollection connect() throws UnknownHostException {
			 // connect to mongodb server
	         MongoClient mongoClient = new MongoClient(HOST_NAME, PORT_NUM);
	         // connect to your databases
	         DB db = mongoClient.getDB(DB_NAME);
			 System.out.println("Mongo: connecting to database 'cmpe283_Project2' ...");
			 
			 // no authentication required
	         /*boolean auth = db.authenticate(myUserName, myPassword);
			 System.out.println("Authentication: "+auth);*/
			 
			// get a collection
	        DBCollection col = db.getCollection(COLLECTION_NAME);
	        System.out.println("Mongo: collection 'vmLogs' selected");
			return col;
		}
		
		public static void aggregaet(DBCollection coll) {
			//TODO: aggregat the data for 5 mins time interval
			
			// get timestamp
			
			// aggregate the value
		}
		
		public static ArrayList<BasicDBObject> retrive(DBCollection coll) {
			
			ArrayList<BasicDBObject> docList = new ArrayList<BasicDBObject>();
			DBCursor cursor = coll.find();
			BasicDBObject doc;
			
			// copy the mem addr to the list
	        while (cursor.hasNext()) {
	        	doc = (BasicDBObject) cursor.next();
				docList.add(doc);
	        }
	        
	        
	        /*for(BasicDBObject json : docList) {
	        	System.out.println(json); 
	        }*/
	        
	        return docList;
		}

	}
	
	private static class MySQLConnector {
		
		// JDBC driver name and database URL
		static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		static final String DB_URL = "jdbc:mysql://localhost/cmpe283_Project2";
		
		//  Database credentials
		static final String USER = "root";
		static final String PASS = "internation";
		static final String TABLE_NAME = "vmLogs";
		
		public static Connection connect() throws ClassNotFoundException, SQLException {
			
			Connection conn = null;
			
			// Register JDBC driver
			Class.forName(JDBC_DRIVER);
			
			// Open a connection
			System.out.println("MySQL: connecting to database 'cmpe283_Project2' ...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("MySQL: connected database successfully...");
			
			return conn;	
		}
		
		public static void insert(Connection conn, ArrayList<BasicDBObject> docList) throws SQLException, ParseException {
			
			Statement stmt = null;
			
			// Execute a query
		    System.out.println("Inserting records into the table...");
		    stmt = conn.createStatement();
		    
		    // doesn't have to specify all the column names
		    /** reference **/
//		    INSERT INTO table_name (column1,column2,column3,...)
//		    VALUES (value1,value2,value3,...);
		    
		    for(BasicDBObject doc : docList) {
		    	
		    	String id = doc.getString("_id");
		    	Timestamp timestamp = parseDate(doc.getString("@timestamp"));
		    	String vmType = doc.getString("vmType");
		    	String vmName = doc.getString("vmName");
		    	String groupInfo = doc.getString("groupInfo");
		    	String nameInfo = doc.getString("nameInfo");
		    	String rollupType = doc.getString("rollupType");
		    	String unitInfo = doc.getString("unitInfo");
		    	Integer value = doc.getInt("value");
		    	
		    	
			    String sql = "INSERT IGNORE INTO " + TABLE_NAME
			    		+ " (id, timestamp, vmType, vmName, groupInfo, nameInfo, rollupType, unitInfo, value)"
			    		+ " VALUES('" + id + "', '" + timestamp + "', '" + vmType	+ "', '" + vmName + "', '" + groupInfo 
			    		+ "', '" + nameInfo + "', '" + rollupType + "', '" + unitInfo + "', " + value + ")";
			    
			    
//			    System.out.println(sql);
			    
			    stmt.executeUpdate(sql);
		    }
		}
		
		/*
		 * format of time zone in mongodb:
		 * "@timestamp" : "\"2014-11-24T05:24:31.484Z\""
		 */
		private static Timestamp parseDate(String timestamp) throws ParseException {
			SimpleDateFormat formatter = new SimpleDateFormat("\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"");
			formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
			java.util.Date date = formatter.parse(timestamp);
			Timestamp sqltimestamp = new Timestamp(date.getTime());
			
			return sqltimestamp;
		}
	}
}