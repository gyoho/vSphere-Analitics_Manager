package logCollector;
import java.util.ArrayList;

import logCollector.*;

import com.vmware.vim25.mo.*;


public class Starter {
	
	private CredentialsHolder credentials;
	private ServiceInstance center;
	private Folder rootFolder;
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
		
		// get specific VM
		vm = new InventoryNavigator(rootFolder).searchManagedEntity("VirtualMachine", vmName);
		
		// get all hosts
		hostList = InstanceGetter.getAllInstance(rootFolder, "HostSystem");
		// get its parent host
		host = VmToHostMapper.getHostOfVm(hostList, vm);
		
		ccm = new CounterIDCounterInfoMapper(center, (VirtualMachine) vm);
	}
	
	public void start() throws Exception {
		long interval = 5*1000;
		
		while(true) {
			System.out.println("Extracting logs...");
			startRealtimePerfMonitor(vm, host);
			
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
