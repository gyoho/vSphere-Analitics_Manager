import java.util.ArrayList;

import components.VmToHostMapper;

import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.mo.*;

import components.*;


public class Starter {
	
	private CredentialsHolder credentials;
	private ServiceInstance center;
	private Folder rootFolder;
	private Datacenter datacenter;
	private ManagedEntity vm;
	private ArrayList<ManagedEntity> hostList;
	private ManagedEntity host;
	private CounterIDCounterInfoMapper ccm;
	
	
	// constructor
	public Starter(String vmName) throws Exception {
		
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
		
		// get specific VM
		vm = new InventoryNavigator(rootFolder).searchManagedEntity("VirtualMachine", vmName);
		
		// get all hosts
		hostList = InstanceGetter.getAllInstance(rootFolder, "HostSystem");
		// get its parent host
		host = VmToHostMapper.getHostOfVm(hostList, vm);
		
		ccm = new CounterIDCounterInfoMapper(center, (VirtualMachine) vm);
	}
	
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
			startRealtimePerfMonitor(vm, host);
			
			System.out.println("Waiting for Logstash to do its job...");
			Thread.sleep(logstashBreak);
			
			System.out.println("Transporting logs from MongoDB to MySQL...");
			MongoToMySQLTransporter.transportData();
			
			System.out.println("Sleeping 5 seconds...\n\n");
			Thread.sleep(interval);
		}		
		
	}
	
	private void startRealtimePerfMonitor(ManagedEntity ... lists ) throws Exception {
		for(ManagedEntity vm : lists) {
			RealtimePerfMonitor.printStats(center, vm, ccm);
		}
	}
}
