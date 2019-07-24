package Main;

import java.util.TreeMap;

import Classes.Generation;
import Classes.Print;
import Classes.TxtReader;
import Classes.TxtReader.typeData;
import eu.recap.sim.models.ApplicationModel.ApplicationLandscape;
import eu.recap.sim.models.InfrastructureModel.Infrastructure;
import eu.recap.sim.models.WorkloadModel.Workload;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Tests {

	public static TreeMap<Double, List<Double>> ramSimu;
	public static TreeMap<Double, List<Double>> ramReal;
	
	public static TreeMap<Double,List<Double>> cpuSimu;
	public static TreeMap<Double,List<Double>> cpuReal;

	public static void main(String[] args) throws FileNotFoundException {

		/*
		 * Simulation of data
		 */
		Infrastructure rim = Generation.GenerateInfrastructure("General infrastructure");
		ApplicationLandscape ram = Generation.GenerateApplicationLandscape(Launcher.NB_APPS, Launcher.NB_PRIMARYSHARDS,
				rim);
		Workload rwm = Generation.GenerateYCSBWorkload(Launcher.NB_PRIMARYSHARDS, ram, 10);
		Launcher launcher = new Launcher(rim, ram, rwm);
		
		ramSimu = launcher.getRecapExperiment().getAllVmsRamUtilizationHistory();
		cpuSimu = launcher.getRecapExperiment().getAllVmsCpuUtilizationHistory();

		/*
		 * Real data
		 */
		ramReal = getData(Launcher.NB_PRIMARYSHARDS,typeData.MemoryUsage);
		cpuReal = getData(Launcher.NB_PRIMARYSHARDS,typeData.CpuLoad);
		
		Print.printMapMonitoring(ramReal);
		Print.printMapMonitoring(ramSimu);

	}
	
	public static void compareData(TreeMap<Double,List<Double>> realData, TreeMap<Double,List<Double>> simuData) {
		//putting both datasets on the same timeline for comparison
		//TODO for now assuming time is simu is in seconds
		
		double pearsonCoeff;
		
		
		
		
	}

	@SuppressWarnings("unchecked")
	public static TreeMap<Double, List<Double>> getData(int numberNodes, typeData type) {
		List<Integer> vms;
		switch (numberNodes) {
		case 3:
			vms = Arrays.asList(111, 144, 164);
			break;
		case 9:
			vms = Arrays.asList(111, 121, 122, 142, 143, 144, 164, 212, 250); // VM 149 not a data node
			break;
		default:
			throw new IllegalArgumentException("numberNodes can only be 3 or 9");
		}

		TreeMap<Double, List<Double>> data = new TreeMap<Double, List<Double>>();
		
		for(int vm:vms) {
			//values for the VM vm, c0=time, c1=date, c2=value
			List<List<Double>> valuesVm = (List<List<Double>>)(List<?>)TxtReader.readMonitoring(numberNodes, type, vm);
			List<Double> times = valuesVm.get(0);
			List<Double> values = valuesVm.get(2);	
			
			//removing first 10 values for VM111 to align with other VMs
			if (vm==111) {				
				for(int index=0;index<10;index++) {
					times.remove(0);
				}
				for(int i=0;i<times.size();i++) {
					times.set(i, times.get(i)-100.0);
				}
				
			}
			
			int indexTime=0;
			for (double time:times) {
				if (!data.containsKey(time)) {
					List<Double> valuesTime = new ArrayList<Double>();
					valuesTime.add(values.get(indexTime));
					data.put(time, valuesTime);
				} else {
					data.get(time).add(values.get(indexTime));
				}
				
				indexTime++;
			}
		}
		
		return data;
		

	}

}
