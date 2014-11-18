import com.vmware.vim25.mo.*;

import components.*;


public class Starter {
	
	private CredentialsHolder credentials;
	private ServiceInstance center;
	private Folder rootFolder;
	private Datacenter datacenter;
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
		String dcName = "DC-Team06";
		datacenter = (Datacenter) new InventoryNavigator(rootFolder).searchManagedEntity("Datacenter", dcName);
		
		// get specific VM
		String vmName = "T06-VM01-Ubn01";
		vm = (VirtualMachine) new InventoryNavigator(rootFolder).searchManagedEntity("VirtualMachine", vmName);
		
		// get Host
		String hostName = "130.65.132.184"; 
		host = (HostSystem) new InventoryNavigator(rootFolder).searchManagedEntity("HostSystem", hostName);
		
		ccm = new CounterIDCounterInfoMapper(center, vm);
	}
	
	public void start() throws Exception {
		
//		RealtimePerfMonitor.printStats(center, vm, ccm);
		
		long interval = 5*1000;
		while(true) {
			RealtimePerfMonitor.printStats(center, vm, ccm);
			Thread.sleep(interval);
		}
	}
}
