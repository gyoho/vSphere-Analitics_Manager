import java.util.ArrayList;

import com.vmware.vim25.mo.*;

import components.*;


public class Starter {
	
	private CredentialsHolder credentials;
	private ServiceInstance center;
	private Folder rootFolder;
	private Datacenter datacenter;
	private ArrayList<ManagedEntity> vmList;
	private ArrayList<ManagedEntity> hostList;
	private HostSystem host;
	private VirtualMachine vm;
	private CounterIDCounterInfoMapper ccm;
	
	
	// constructor
	public Starter() throws Exception {
		
		/*** All initial configuration ***/
		// Instantiate credentials
		credentials = new CredentialsHolder();
		
		// get service instance in vCenter
		center = ServiceInstanceGetter.getServiceInstance
				(credentials.getvCenterIpAddr(), credentials.getUsrName(), credentials.getPasswd());
		
		// get root folder of the vCenter
		rootFolder = RootFolderGetter.getRootFolder(center);
		
		// get datacenter
		String dcName = "DC_Team06";
		datacenter = (Datacenter) new InventoryNavigator(rootFolder).searchManagedEntity("Datacenter", dcName);
		
		
		// get all VMs in the vCenter, excluding hosts
		vmList = InstanceGetter.getAllInstance(rootFolder, "VirtualMachine");
		// get all hosts
		hostList = InstanceGetter.getAllInstance(rootFolder, "HostSystem");
		
		
		/*// get specific VM
		String vmName = "T06_VM01_Ubn01";
		vm = (VirtualMachine) new InventoryNavigator(rootFolder).searchManagedEntity("VirtualMachine", vmName);		
		// get Host
		String hostName = "130.65.132.181"; 
		host = (HostSystem) new InventoryNavigator(rootFolder).searchManagedEntity("HostSystem", hostName);*/
		
		ccm = new CounterIDCounterInfoMapper(center, ((VirtualMachine) vmList.get(0)));
	}
	
	@SuppressWarnings("unchecked")
	public void start() throws Exception {
		
//		RealtimePerfMonitor.printStats(center, vm, ccm);
		
		long logstashBreak = 2*100; 
		long interval = 5*1000;
		/* TEST
		   while(true) {
			RealtimePerfMonitor.printStats(center, vm, ccm);
			Thread.sleep(interval);
		   }
		*/
		
		while(true) {
			System.out.println("Extracting logs...");
			startRealtimePerfMonitor(vmList, hostList);
			
			System.out.println("Waiting for Logstash to do its job...");
			Thread.sleep(logstashBreak);
			
			System.out.println("Transporting logs from MongoDB to MySQL...");
			MongoToMySQLTransporter.transportData();
			
			System.out.println("Sleeping 5 seconds...\n\n");
			Thread.sleep(interval);
		}		
		
	}
	
	private void startRealtimePerfMonitor(ArrayList<ManagedEntity> ... vmLists ) throws Exception {
		for(ArrayList<ManagedEntity> vmList : vmLists) {
			for(ManagedEntity vm: vmList) {
				RealtimePerfMonitor.printStats(center, vm, ccm);
			}
		}
	}
}
