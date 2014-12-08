 /**
   * Responsibility: get the root folder with the service instance provided
   *
   * @param: serviceInstance
   * @return: rootFolder
   *
   */

package logCollector;

import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.ServiceInstance;

public class RootFolderGetter {
	
	public static Folder getRootFolder(ServiceInstance serviceInstance) {
		Folder rootFolder = serviceInstance.getRootFolder();
//		String name = rootFolder.getName();
//		System.out.println("root:" + name);
		return rootFolder;
	}

}
