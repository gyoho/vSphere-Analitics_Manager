 /**
   * Responsibility: hold all credentials
   *
   * @param none
   * @return getter for all credentials
   *
   */


package logCollector;

public class CredentialsHolder {
	
	private String managementCenterIpAddr;
	private String vCenterIpAddr;
	private String resourcePoolName;
	private String usrName;
	private String passwd;
	// if user powers it off, it takes time to reflect the change
	private int timeout = 7*1000;
	
	public CredentialsHolder() {
		managementCenterIpAddr = "130.65.132.14";
		vCenterIpAddr = "130.65.132.180";
		resourcePoolName = "Team06_vHOSTS";
		usrName = "administrator";
		passwd = "12!@qwQW";
	}
	
	public String getManagementCenterIpAddr() {
		return managementCenterIpAddr;
	}
	
	public String getvCenterIpAddr() {
		return vCenterIpAddr;
	}
	
	public String getResourcePoolName() {
		return resourcePoolName;
	}
	
	public String getUsrName() {
		return usrName;
	}
	
	public String getPasswd() {
		return passwd;
	}
	
	public int getTimeout() {
		return timeout;
	}
}
