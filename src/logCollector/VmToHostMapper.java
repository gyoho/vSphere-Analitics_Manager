 /**
   * Responsibility: find the host for the VM
   *
   * @param vm: VM instance
   * @return host: ManagedEntity instance
   *
   */

package logCollector;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.VirtualMachine;

public class VmToHostMapper {
	
	public static ManagedEntity getHostOfVm(ArrayList<ManagedEntity> hostList, ManagedEntity vm) throws InvalidProperty, RuntimeFault, RemoteException {
		/** VM to vHost **/
		
		// for each host, get all VMs belongs to the host
		for(ManagedEntity host : hostList) {
//			System.out.println("host name: " + host.getName());

			// get all the VMs for the host
			ArrayList<ManagedEntity> vmList = getAllVMsOfHost(host);

			// for each vm in the host list, check if the name match with the given vm's name
			for(ManagedEntity vms : vmList) {
				if(vms.getName().equals(vm.getName())) {
					return host;
				}
			}
		}
		System.out.println("something really wrong...");
		return null;		
	}
	
	public static ManagedEntity getHostVMOfVM(ArrayList<ManagedEntity> vmListInRecPool, ArrayList<ManagedEntity> hostList, ManagedEntity vm) throws InvalidProperty, RuntimeFault, RemoteException {
		
		/** VM to HostVM **/
		
		// for each VM in the resource pool
		// match the name of the last 8 chars with the VM's host name
		for(ManagedEntity hostVM : vmListInRecPool) {
			
			// get the VM's name
			String hostVMName = ((VirtualMachine)hostVM).getName();
//			System.out.println("host vm name: " + vmName);
			
			// get the last 8 chars of its IP address
			String lastIPAddr = hostVMName.substring(hostVMName.length()-8);
//			System.out.println("host vm name last 8 chars: " + lastIPAddr);
			
			// for each host, check if the host's name ends with the 8 chars
			for(ManagedEntity host : hostList) {
				
				// get the host's name
				String hostName = ((HostSystem)host).getName();
//				System.out.println("host name: " + hostName);
				
				// check if matches
				if(hostName.endsWith(lastIPAddr)) {
					
					/*** register the VMs of the host ***/
					// get all the VMs for the host
					ArrayList<ManagedEntity> vmList = getAllVMsOfHost(host);
					
					// for each vm in the host list, check if the name match with the given vm's name
					for(ManagedEntity vms : vmList) {
						if(vms.getName().equals(vm.getName())) {
							return hostVM;
						}
					}
				}
			}
		}
		
		System.out.println("something really wrong...");
		return null;		
	} 
	
	// get all the VMs for the host
	public static ArrayList<ManagedEntity> getAllVMsOfHost(ManagedEntity host) throws InvalidProperty, RuntimeFault, RemoteException {
		ManagedEntity[] vmArray = ((HostSystem)host).getVms();
		return (new ArrayList<ManagedEntity>(Arrays.asList(vmArray)));
	}
}
