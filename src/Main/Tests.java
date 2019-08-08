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

	public static TreeMap<Double, List<Double>> cpuSimu;
	public static TreeMap<Double, List<Double>> cpuReal;

	public static void main(String[] args) throws FileNotFoundException, InterruptedException {

		/*
		 * Simulation of data
		 */
		Infrastructure rim = Generation.GenerateInfrastructure("General infrastructure");
		ApplicationLandscape ram = Generation.GenerateAppLandscape(Launcher.NB_APPS, Launcher.NB_PRIMARYSHARDS,
				rim);
		Workload rwm = Generation.GenerateYCSBWorkload(Launcher.NB_PRIMARYSHARDS, ram, 83-10, 10);
		Launcher launcher = new Launcher(rim, ram, rwm);

		ramSimu = launcher.getRecapExperiment().getAllVmsRamUtilizationHistory();
		cpuSimu = launcher.getRecapExperiment().getAllVmsCpuUtilizationHistory();

		/*
		 * Real data
		 */
		ramReal = getData(Launcher.NB_PRIMARYSHARDS, typeData.MemoryUsage);
		cpuReal = getData(Launcher.NB_PRIMARYSHARDS, typeData.CpuLoad);

		//Print.printMapMonitoring(ramReal);
		//Print.printMapMonitoring(ramSimu);
		 Print.printMapMonitoring(cpuReal);
		 Print.printMapMonitoring(cpuSimu);
		
		//Print.printMapMonitoring(gapStrings(cpuReal));

	}
	
	
	/**
	 * For testing, show the gap to mean for each VM, allows to see which VMs are above mean usage </br>
	 * Only useful for real data
	 */
	public static TreeMap<Double, List<String>> gapStrings(TreeMap<Double, List<Double>> simuData) {
		double[] moys = new double[simuData.get(simuData.firstKey()).size()];
		for (int i = 0; i < moys.length; i++) { moys[i] = 0; }
		
		for (double time : simuData.keySet()) {
			for(int vm=0;vm<simuData.get(time).size();vm++) {
				moys[vm]+=simuData.get(time).get(vm);
			}
		}
		
		for(int i=0;i<moys.length;i++) {
			moys[i]=moys[i]/simuData.size();
		}
		
		TreeMap<Double,List<String>> res = new TreeMap<Double, List<String>>();
		for(double key:simuData.keySet()) {
			List<String> add = new ArrayList<String>();
			
			for(int vm=0;vm<simuData.get(key).size();vm++) {
				String s = "";
				if (key==simuData.firstKey())
					s+=moys[vm];
				double diff = simuData.get(key).get(vm)-moys[vm];
				
				s+=(diff<0)?" - ":" + ";
				
				s+=String.format("%.4f",Math.abs(diff));
				
				add.add(s);
			}
			
			res.put(key, add);
		}
		return res;
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

		for (int vm : vms) {
			// values for the VM vm, c0=time, c1=date, c2=value
			List<List<Double>> valuesVm = (List<List<Double>>) (List<?>) TxtReader.readMonitoring(numberNodes, type,
					vm);
			List<Double> times = valuesVm.get(0);
			List<Double> values = valuesVm.get(2);

			// removing first 10 values for VM111 to align with other VMs
			if (vm == 111) {
				for (int index = 0; index < 10; index++) {
					times.remove(0);
				}
				for (int i = 0; i < times.size(); i++) {
					times.set(i, times.get(i) - 100.0);
				}

			}

			int indexTime = 0;
			for (double time : times) {
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
