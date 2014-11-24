package components;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.sql.*;
import java.util.ArrayList;

public class MongoToMySQLTransporter {
	
	private static DBCollection collection;
	private static Connection database;
	private static ArrayList<BasicDBObject> documentList;
	
	public static void runMongoDB() throws UnknownHostException {
		collection = MongoDBConnector.connect();
		documentList = MongoDBConnector.retrive(collection);	
	}
	
	public static void runMySQL() throws ClassNotFoundException, SQLException {
		database = MySQLConnector.connect();
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
			 System.out.println("Connect to database 'cmpe283_Project2' successfully");
			 
			 // no authentication required
	         /*boolean auth = db.authenticate(myUserName, myPassword);
			 System.out.println("Authentication: "+auth);*/
			 
			// get a collection
	        DBCollection col = db.getCollection(COLLECTION_NAME);
	        System.out.println("Collection 'vmLogs' selected successfully");
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
	        
	        /** test **/
	        for(BasicDBObject json : docList) {
	        	System.out.println(json); 
	        }
	        
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
			System.out.println("Connecting to database 'cmpe283_Project2' ...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("Connected database successfully...");
			
			return conn;	
		}
		
		public static void insert(Connection conn, ArrayList<BasicDBObject> docList) throws SQLException {
			
			Statement stmt = null;
			
			// Execute a query
		    System.out.println("Inserting records into the table...");
		    stmt = conn.createStatement();
		    
		    // doesn't have to specify all the column names
		    /** reference **/
//		    INSERT INTO table_name (column1,column2,column3,...)
//		    VALUES (value1,value2,value3,...);
		    
		    for(BasicDBObject doc : docList) {
			    String sql = "INSERT INTO " + TABLE_NAME
			    		+ "(id, timestamp, vmType, vmName, groupInfo, nameInfo, rollupType, unitInfo, value) "
			    		+ "VALUES(" + doc.getString("_id") + doc.getDate("timestamp") + doc.getString("vmType")
			    		+ doc.getString("vnName") + doc.getString("groupInfo") + doc.getString("nameInfo")
			    		+ doc.getString("rollupType") + doc.getString("unitInfo") + doc.getInt("timestamp") + ")";
			    stmt.executeUpdate(sql);
		    }
		}
	}
}