 /**
   * Responsibility: get all VM instance within the folder
   *
   * @param: rootFolder
   * @param String instanceType: Datacenter, HostSystem, VirtualMachine
   * @return ManagedEntity[] instanceList: all instances of VM
   *
   * Use ManagedEntity and cast it later according to its type
   *
   */

package logCollector;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;

import com.vmware.vim25.*;
import com.vmware.vim25.mo.*;

public class InstanceGetter {
	
	public static ArrayList<ManagedEntity> getAllInstance(Folder rootFolder, String instanceType) throws InvalidProperty, RuntimeFault, RemoteException {
		// Access all instances
		ManagedEntity[] instanceArray = new InventoryNavigator(rootFolder).searchManagedEntities(instanceType);

		ArrayList<ManagedEntity> instanceList = new ArrayList<ManagedEntity>(Arrays.asList(instanceArray));
		
		if(instanceList == null || instanceList.size() == 0) {
			System.out.println("No " + instanceType + " exists in the inventory");
		}
		
		return instanceList;
	}

}
