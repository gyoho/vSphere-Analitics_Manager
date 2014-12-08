 /**
   * Responsibility: get the service instance with the IP address
   *
   * @param: ipAddr
   * @param: usrName
   * @param: passwd
   * @return: serviceInstance
   *
   */
   
package logCollector;

import java.net.URL;
import com.vmware.vim25.mo.ServiceInstance;

public class ServiceInstanceGetter {

	public static ServiceInstance getServiceInstance(String ipAddr, String usrName, String passwd) throws Exception {
		// get center
		URL url = new URL("https://" + ipAddr + "/sdk");
		ServiceInstance serviceInstance = new ServiceInstance(url, usrName, passwd, true);
		return serviceInstance;
	}

}






