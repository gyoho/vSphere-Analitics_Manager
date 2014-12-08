package dataTransporter;


public class Controller {
	
	// 1 minutes
	static long interval = 1*60*1000;
	static int hourlyCount = (int) (60*60*1000 / interval);
	static boolean isAnHour;
	static int timeTracker = 0;

	public static void main(String[] args) throws Exception {
		while(true) {
			System.out.println("Transporting logs from MongoDB to MySQL...");
			MongoToMySQLTransporter.transportData();
			
			System.out.println("Sleeping 1 mins...\n\n");
			Thread.sleep(interval);
			
			timeTracker++;
			
			// if passes an hour
			if(timeTracker >= hourlyCount) {
				System.out.println("\nPrepare for hourly summary\n");
				// do the hourly process
				MongoToMySQLTransporter.transportHourlyData();
				
				// reset timer
				timeTracker = 0;
			}
		}
	}
}




