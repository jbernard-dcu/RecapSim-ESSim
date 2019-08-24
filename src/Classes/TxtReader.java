package Classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.math3.distribution.LogNormalDistribution;

import Classes.ReaderUtils.typeData;
import Distribution.LogNormalFunc;

public class TxtReader {

	///////////////////////////////////////////////////////////////////////////////////////
	///////////////// OTHER METHODS
	///////////////////////////////////////////////////////////////////////////////////////

	@SuppressWarnings("unchecked")
	private static List<Double> getWaitingTimesReads(int nbNodes, int vm) {

		List<List<Object>> monitoringData = new MonitoringReader(nbNodes, vm, typeData.DiskIOReads).getData();

		List<Double> timestamps = (List<Double>) (List<?>) monitoringData.get(0);
		List<Double> values = (List<Double>) (List<?>) monitoringData.get(2);

		int previousOc = 0;

		List<Double> waitingTimes = new ArrayList<>();
		for (int time = 0; time < timestamps.size(); time++) {
			if (values.get(time) != 0) {
				waitingTimes.add(timestamps.get(time) - timestamps.get(previousOc));
				previousOc = time;
			}
		}

		return waitingTimes;
	}

	@SuppressWarnings("unchecked")
	private static List<Double> getValuesNonZeroReads(int nbNodes, int vm) {

		List<Double> values = (List<Double>) (List<?>) new MonitoringReader(nbNodes, vm, typeData.DiskIOReads).getData()
				.get(2);
		return values.stream().filter(value -> value > 0).collect(Collectors.toList());
	}

	///////////////////////////////////////////////////////////////////////////////////////
	///////////////// PUBLIC METHODS
	///////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Calculates the average weights of operations of each type, based on the
	 * average latency of requests of each type</br>
	 * These weights can help us calculate MI necessary to execute requests
	 */
	public static Map<String, Double> calculateCyclesType(List<List<Object>> requests) {

		Map<String, Double> tam = new HashMap<String, Double>();
		Map<String, Integer> sizes = new HashMap<String, Integer>();

		for (int time = 0; time < requests.get(0).size(); time++) {
			String type = (String) requests.get(1).get(time);
			double avgLat = (double) requests.get(2).get(time);

			if (!sizes.containsKey(type))
				sizes.put(type, 0);

			if (tam.containsKey(type))
				avgLat += tam.get(type);

			tam.put(type, avgLat);
			sizes.put(type, sizes.get(type) + 1);
		}

		double total = 0;
		for (String type : tam.keySet()) {
			tam.put(type, tam.get(type) / sizes.get(type));
			total += tam.get(type);
		}

		for (String type : tam.keySet()) {
			tam.put(type, tam.get(type) / total);
		}

		return tam;
	}

	/**
	 * Method to calculate the approx. repartition of the requests among the data
	 * nodes, based on specified type of data
	 */
	public static double[] calculateRepartNodes(int nbNodes, typeData type) {

		int[] vms = ReaderUtils.getMonitoringVmsList(nbNodes);

		// getting cpuLoads
		List<List<Double>> values = new ArrayList<>();
		for (int vm : vms) {
			@SuppressWarnings("unchecked")
			List<Double> add = (List<Double>) (List<?>) readMonitoring(nbNodes, type, vm).get(2);

			if (vm == 111)
				add = add.subList(10, add.size()); // removing the 10 first values of VM111 to align timestamps
			values.add(add);
		}

		// calculating normalized values
		List<List<Double>> normCpuLoads = new ArrayList<List<Double>>();

		for (int vm = 0; vm < values.size(); vm++) {

			List<Double> add = new ArrayList<Double>();

			for (int time = 0; time < values.get(vm).size(); time++) {
				double sum = 0;
				for (int vm_bis = 0; vm_bis < values.size(); vm_bis++) {
					sum += values.get(vm_bis).get(time);
				}

				add.add(values.get(vm).get(time) * ((sum == 0) ? 1 : 1 / sum));
			}
			normCpuLoads.add(add);
		}

		// average for each VM of the normalized values
		double[] distribution = new double[vms.length];

		for (int vm = 0; vm < normCpuLoads.size(); vm++) {
			double sum = 0;
			for (int time = 0; time < normCpuLoads.get(vm).size(); time++) {
				sum += normCpuLoads.get(vm).get(time);
			}

			distribution[vm] = sum / normCpuLoads.get(vm).size();
		}

		return distribution;

	}

	/**
	 * 0=write mode, 1=read mode
	 * 
	 * @param type
	 * @param vm
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static double[] getAvgValuesVM(int nbNodes, typeData type, int vm) {
		List<Double> valuesW = (List<Double>) (List<?>) cleanDataset(nbNodes, type, loadMode.WRITE, vm).get(2);
		List<Double> valuesR = (List<Double>) (List<?>) cleanDataset(nbNodes, type, loadMode.READ, vm).get(2);

		return new double[] { valuesW.stream().mapToDouble(s -> s).average().getAsDouble(),
				valuesR.stream().mapToDouble(s -> s).average().getAsDouble() };

	}

	/**
	 * returns a double[] of size 2 containing the average of averages of usage of
	 * VMs in specified type of data VMs for the loading phase (0) and the
	 * transaction phase (1)
	 * 
	 * @param nbNodes
	 * @param type
	 * @return
	 */
	public static double[] getAvgValuesAll(int nbNodes, typeData type) {
		double[] res = new double[] { 0, 0 };

		int[] vms = ReaderUtils.getMonitoringVmsList(nbNodes);

		double[] values;
		for (int vm : vms) {
			values = getAvgValuesVM(nbNodes, type, vm);
			res[0] += values[0];
			res[1] += values[1];
		}
		res[0] /= vms.length;
		res[1] /= vms.length;

		return res;
	}

	/**
	 * returns the fitDistribution for the given parameters and the specified
	 * loadMode
	 * 
	 * @param nbDNs
	 * @param componentId
	 * @param type
	 * @param precision
	 * @param load
	 * @return
	 */
	public static double[] getParamsDist(int nbDNs, int componentId, typeData type, double precision, loadMode load) {
		if (componentId < 3 || componentId > 2 + nbDNs)
			throw new IllegalArgumentException(
					"componentId must point to one of the datanodes (3 <= componentId <= 2+nbDNs)");

		// TODO check typeData to avoid having less than two modes

		int[] vms = ReaderUtils.getMonitoringVmsList(nbDNs);

		@SuppressWarnings("unchecked")
		List<Double> dataset = (List<Double>) (List<?>) readMonitoring(nbDNs, type, vms[componentId - 3]).get(2);

		// Calculating specified freqDist
		TreeMap<Double, Double> freqDist = getFrequencyDist(dataset, precision);
		// Getting sub distributions
		List<Map<Object, Double>> modesDists = getModesList(freqDist, precision);
		// Getting the right dist based on loadMode. TODO check that the first and last
		// modes are always the good ones
		System.out.println(modesDists.size());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		switch (load) {
		case WRITE:
			return estimateGaussianDist(modesDists.get(0));
		case READ:
			return estimateGaussianDist(modesDists.get(modesDists.size() - 1));
		}

		return null;
	}

	///////////////////////////////////////////////////////////////////////////////////////
	///////////////// PRIVATE METHODS
	///////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Returns estimators of the parameters of a normal distribution fitting the
	 * dist given. Does not check if the data given really has a normal
	 * distribution before calculation
	 * 
	 * @param dist
	 * @return
	 */
	private static double[] estimateGaussianDist(Map<Object, Double> dist) {
		List<Double> listKeyset = (List<Double>) (List<?>) (new ArrayList<>(dist.keySet()));
		double avg = listKeyset.stream().mapToDouble(s -> (Double) s).average().getAsDouble();

		double meanSquares = listKeyset.stream().mapToDouble(s -> Math.pow(s, 2)).average().getAsDouble();
		double std = Math.sqrt(meanSquares - Math.pow(avg, 2));

		return new double[] { avg, std };

	}

	/**
	 * Plots and returns the histogram of the specified data, the precision must be
	 * set
	 * 
	 * @param nbNodes
	 * @param type
	 * @param vm
	 */
	@SuppressWarnings("unchecked")
	private static TreeMap<Double, Double> getFrequencyDist(List<Double> dataset, double precision) {

		TreeMap<Double, Double> res = new TreeMap<Double, Double>();
		final int nbClasses = 1 + (int) (Collections.max(dataset) / precision);

		for (int i = 0; i <= nbClasses; i++) {
			res.put(precision * i, 0.);
		}

		for (double value : dataset) {
			double key = precision * (int) (value / precision);
			res.put(key, res.get(key) + 1);
		}

		printHistogram(res, 0);

		return res;
	}

	private static void printHistogram(TreeMap<Double, Double> freq, long waitingTimeMillis) {
		for (double key : freq.keySet()) {
			String s = "";
			for (int i = 0; i < freq.get(key).intValue(); i++) {
				s += "o";
			}
			System.out.println(key + " " + s);
			try {
				Thread.sleep(waitingTimeMillis);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Splits the observed frequency distribution in many new frequency
	 * distributions corresponding o the different modes
	 * 
	 * @param freqDist
	 * @param precision
	 * @return
	 */
	private static List<Map<Object, Double>> getModesList(TreeMap<Double, Double> freqDist, double precision) {

		double startCrop = 0;
		int cN = 0;
		List<Map<Object, Double>> foundCrops = new ArrayList<>();
		for (double key : freqDist.keySet()) {
			if (freqDist.get(key) != 0) {
				if (cN == 1)
					startCrop = key;
				cN++;
			} else {
				if (cN >= 3) {
					List<Double> filter = new ArrayList<>();
					for (double cle = Math.max(0, startCrop - precision); cle < Math.min(key + precision,
							freqDist.keySet().size()); cle += precision) {
						filter.add(cle);
					}

					foundCrops.add(filter.stream().filter(freqDist::containsKey)
							.collect(Collectors.toMap(k -> k, freqDist::get)));
				}

				cN = 0;

			}
		}

		// TODO merge maps in the case they are not disjointed

		return foundCrops;
	}

}