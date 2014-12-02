package components;

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClientURI;

import java.net.UnknownHostException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
	
	public static void transportData() throws ClassNotFoundException, SQLException, ParseException, UnknownHostException {
		documentList =  MongoDBConnector.getCollection();
		MySQLConnector.insert(documentList);
	}
	
	
	private static class MongoDBConnector {
	
		static final String DB_NAME = "cmpe283_project2";
		static final String COLLECTION_NAME = "vmLogs";
		
		public static ArrayList<BasicDBObject> getCollection() throws UnknownHostException {
			
			// Standard URI format: mongodb://[dbuser:dbpassword@]host:port/dbname 
	        MongoClientURI uri  = new MongoClientURI("mongodb://gyoho:team6@ds053310.mongolab.com:53310/cmpe283_project2"); 
	        MongoClient client = new MongoClient(uri);
	        DB db = client.getDB(DB_NAME);
	        System.out.println("Mongo: connected to the remote database " + DB_NAME);
			 
			// get a collection
	        DBCollection col = db.getCollection(COLLECTION_NAME);
	        System.out.println("Mongo: collection " + COLLECTION_NAME + " selected");
	        
			
			ArrayList<BasicDBObject> docList = new ArrayList<BasicDBObject>();
			DBCursor cursor = col.find();
			BasicDBObject doc;
			
			// copy the mem addr to the list
	        while (cursor.hasNext()) {
	        	doc = (BasicDBObject) cursor.next();
				docList.add(doc);
	        }
	        
	        /** close the connection, otherwise too many threads accessing the db
	         *  MongoLab has limitation for concurrent accesses **/
	        cursor.close();
	        client.close();
	        
	       
	        return docList;
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
		static final String TABLE_NAME = "vmLogs";
		
		/** Localhost 
		 * @throws ClassNotFoundException **/
		// JDBC driver name and database URL
		/*static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		static final String DB_URL = "jdbc:mysql://localhost/cmpe283_Project2";
		
		//  Database credentials
		static final String USER = "root";
		static final String PASS = "internation";
		static final String TABLE_NAME = "vmLogs";*/
		
		public static void insert(ArrayList<BasicDBObject> docList) throws SQLException, ParseException, ClassNotFoundException {
			
			Connection conn = null;
			
			// Register JDBC driver
			Class.forName(JDBC_DRIVER);
			
			// Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("MySQL: connected database successfully...");
		
			
			/** Execute Statement **/
			Statement stmt = null;
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

			    
			    stmt.executeUpdate(sql);
		    }
		    
		    // Execute a query
		    System.out.println("Inserted records into the table");
		    
		    /** close the connection, otherwise too many threads accessing the db
	         *  ClearDB has limitation for concurrent accesses **/
		    stmt.close();
		    conn.close();
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