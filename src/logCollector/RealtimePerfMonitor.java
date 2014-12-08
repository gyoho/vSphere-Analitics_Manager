
package logCollector;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Timestamp;

import com.vmware.vim25.PerfCounterInfo;
import com.vmware.vim25.PerfEntityMetricBase;
import com.vmware.vim25.PerfEntityMetricCSV;
import com.vmware.vim25.PerfMetricId;
import com.vmware.vim25.PerfMetricSeriesCSV;
import com.vmware.vim25.PerfProviderSummary;
import com.vmware.vim25.PerfQuerySpec;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.PerformanceManager;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.VirtualMachine;


public class RealtimePerfMonitor {
	
	@SuppressWarnings("resource")
	public static void printStats(ServiceInstance si, ManagedEntity vm, CounterIDCounterInfoMapper ccm) throws Exception {
		
		/** Create log file **/
		FileOutputStream out;
		PrintStream ps;
		 
		// true => append
		out = new FileOutputStream("/home/administrator/logstash-1.4.2/cmpe283_project2/stats.txt", true);
		// Connect print stream to the output stream
		ps = new PrintStream(out);
		
	        
	    /**Stats config**/   
		PerformanceManager perfMgr = si.getPerformanceManager();
		
		// find out the refresh rate for the virtual machine
		PerfProviderSummary pps = perfMgr.queryPerfProviderSummary(vm);
		int refreshRate = pps.getRefreshRate().intValue();
		
		/*
		 * Use <group>.<name>.<ROLLUP-TYPE> path specification to identify counters.
		 */
		String[] counterNames = new String[] {"cpu.usage.average", "mem.usage.average", "datastore.read.average", "datastore.write.average", "net.usage.average"};
		
		// specific stats
		PerfMetricId[] pmis = createPerfMetricId(counterNames, ccm);
		
	
		/**
		 * Create the query specification for queryPerf().
		 * Specify ONLY 1 value showing up
		 **/
		PerfQuerySpec qSpec = createPerfQuerySpec(vm, pmis, 1, refreshRate);
		
		
		/**
		 * Call queryPerf()
		 *
		 * QueryPerf() returns the statistics specified by the provided
		 * PerfQuerySpec objects. When specified statistics are unavailable -
		 * for example, when the counter doesn't exist on the target
		 * ManagedEntity - QueryPerf() returns null for that counter.
		 **/
		PerfEntityMetricBase[] retrievedStats = perfMgr.queryPerf(new PerfQuerySpec[] {qSpec});
		
		
		
		/**
		 * Cycle through the PerfEntityMetricBase objects. Each object contains
		 * a set of statistics for a single ManagedEntity.
		 **/
		for(PerfEntityMetricBase singleEntityPerfStats : retrievedStats) {
			/*
			 * Cast the base type (PerfEntityMetricBase) to the csv-specific sub-class.
			 */
			PerfEntityMetricCSV entityStatsCsv = (PerfEntityMetricCSV)singleEntityPerfStats;
			/* Retrieve the list of sampled values. */
			PerfMetricSeriesCSV[] metricsValues = entityStatsCsv.getValue();
			if(metricsValues == null) {
				System.out.println("No stats retrieved. " + "Check whether the virtual machine is powered on.");
				throw new Exception();
			}
			
			
			/** 
			 * Output format:
			 * Timestamp VMType VMName GroupInfo NameInfo rollupType UnitInfo value
			**/
			
			
			/**
			 * Retrieve time interval information (PerfEntityMetricCSV.sampleInfoCSV).
			 **/
			
			/*String csvTimeInfoAboutStats = entityStatsCsv.getSampleInfoCSV();
			// Print the time and interval information
			ps.println("Collection: interval (seconds),time (yyyy-mm-ddThh:mm:ssZ)");
			ps.println(csvTimeInfoAboutStats);*/
			java.util.Date date = new java.util.Date();
			
			
		
			/**
			 * Cycle through the PerfMetricSeriesCSV objects. Each object contains
			 * statistics for a single counter on the ManagedEntity.
			 **/
			for(PerfMetricSeriesCSV csv : metricsValues) {
				/*
				 * Use the counterId to obtain the associated PerfCounterInfo object
				 */
				PerfCounterInfo pci = ccm.get(csv.getId().getCounterId());
				/* Print out the metadata for the counter. */
				
				ps.print(new Timestamp(date.getTime()) + " ");
				if(vm instanceof VirtualMachine) {
					ps.print("VirtualMachine " + vm.getName() + " ");
				} else if(vm instanceof HostSystem) {
					ps.print("HostSystem " + vm.getName() + " ");
				}
				ps.print(pci.getGroupInfo().getKey() + " " + pci.getNameInfo().getKey() + " " + pci.getRollupType() + " " + pci.getUnitInfo().getKey() + " ");
				
				if(Double.parseDouble(csv.getValue()) < 0) {
					ps.print(0);
				}
				else {
					if(pci.getGroupInfo().getKey().equals("cpu") || pci.getGroupInfo().getKey().equals("mem")) {
						ps.print((Double.parseDouble(csv.getValue())/100) + "\n");
					}
					else {
						ps.print(csv.getValue() + "\n");
					}
				}
			}
		}
		
		ps.close();
	  }
	
	  private static PerfMetricId[] createPerfMetricId(String[] counterNames, CounterIDCounterInfoMapper ccm) {
		PerfMetricId[] pmis = new PerfMetricId[counterNames.length];
			
		for(int i=0; i<counterNames.length; i++) {
			
			// Create the PerfMetricId object for the counterName.
			// Use an asterisk to select all metrics associated with counterId (instances and rollup).
			 
			PerfMetricId mid = new PerfMetricId();
			
			// Get the ID for this counter. 
			mid.setCounterId(ccm.get(counterNames[i]));
			mid.setInstance("*");
			pmis[i] = mid;
		}
		  return pmis;
	  }
	
	  private static PerfQuerySpec createPerfQuerySpec(ManagedEntity me, PerfMetricId[] metricIds, int maxSample, int interval) {
	    PerfQuerySpec qSpec = new PerfQuerySpec();
	    qSpec.setEntity(me.getMOR());
	    // set the maximum of metrics to be return
	    // only appropriate in real-time performance collecting
	    qSpec.setMaxSample(new Integer(maxSample));
	    qSpec.setMetricId(metricIds);
	    // optionally you can set format as "normal"
	    qSpec.setFormat("csv");
	    // set the interval to the refresh rate for the entity
	    qSpec.setIntervalId(new Integer(interval));
	 
	    return qSpec;
	  }
}