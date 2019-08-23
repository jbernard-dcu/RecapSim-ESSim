package Classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

import Distribution.LogNormalFunc;

public class TxtReader {

	public static void main(String[] args) {
		double precision = 0.25;

		TreeMap<Double, Double> freqDist = getFrequencyDist(9, typeData.NetworkReceived, 142, precision);

		for (Map<Object, Double> dist : getModesList(freqDist, precision)) {
			System.out.println(dist.toString());
			System.out.println(Arrays.toString(fitDistribution(dist)));
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////
	///////////////// ENUMS
	///////////////////////////////////////////////////////////////////////////////////////

	public enum typeData {
		CpuLoad, DiskIOReads, DiskIOWrites, MemoryUsage, NetworkReceived, NetworkSent
	}

	public enum loadMode {
		WRITE, READ;
	}

	///////////////////////////////////////////////////////////////////////////////////////
	///////////////// PUBLIC METHODS
	///////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Builds and returns the filepath String of the monitoring file of nbNodes
	 * cluster and typeData values
	 * 
	 * @param nbNodes
	 * @param type
	 * @return
	 */
	public static String getFilePath(int nbNodes, typeData type, int vm) {
		String path = "/elasticsearch_nodes-" + nbNodes + "_replication-3/nodes-" + nbNodes
				+ "_replication-3/evaluation_run_2018_11_25-";
		if (nbNodes == 3)
			path += "19_10/monitoring/";
		if (nbNodes == 9)
			path += "22_40/monitoring/";

		String fileName;
		switch (type) {
		case CpuLoad:
			fileName = "cpuLoad";
			break;
		case DiskIOReads:
			fileName = "disk-io-reads";
			break;
		case DiskIOWrites:
			fileName = "disk-io-writes";
			break;
		case MemoryUsage:
			fileName = "memory-usage";
			break;
		case NetworkReceived:
			fileName = "network-received";
			break;
		case NetworkSent:
			fileName = "network-sent";
			break;
		default:
			fileName = "";
		}
		fileName += "-node-134.60.64." + vm + ".txt";
		return System.getProperty("user.dir") + File.separator + path + File.separator + fileName;
	}

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

		int[] vms = getMonitoringVmsList(nbNodes);

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
	 * 0=date, 1=type, 2=latency</br>
	 * writeOrRead = "W" or "R"
	 */
	public static List<List<Object>> getRequestsFromFile(int nbNodes, loadMode writeOrRead) {

		// Building the filepath
		String path = "/elasticsearch_nodes-" + nbNodes + "_replication-3/nodes-" + nbNodes
				+ "_replication-3/evaluation_run_2018_11_25-";

		// Checking that nbNodes is equal to 3 or 9
		switch (nbNodes) {
		case 3:
			path += "19_10/data/";
			break;
		case 9:
			path += "22_40/data/";
			break;
		default:
			throw new IllegalArgumentException("nbNodes cas only be 3 or 9 for file reading methods");
		}

		// Checking that writeOrRead is equal to "W" or "R"
		String fileName = "";
		switch (writeOrRead) {
		case WRITE:
			fileName = "load.txt";
			break;
		case READ:
			fileName = "transaction.txt";
			break;
		default:
			throw new IllegalArgumentException("writeOrRead can only be 'W' or 'R' for file reading methods");
		}

		String line = null;

		List<List<Object>> validRequest = new ArrayList<List<Object>>();

		for (int field = 0; field < 3; field++) {
			validRequest.add(new ArrayList<Object>());
		}

		try {
			File file = new File(System.getProperty("user.dir") + File.separator + path + File.separator + fileName);

			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			long previousDate = 0;

			while ((line = bufferedReader.readLine()) != null) {

				if (line.startsWith("2018") && !line.contains("Thread") && line.contains("[")) {

					// Date
					int start = 11;
					int hours = Integer.parseInt(getWord(line, start, ":"));
					int minutes = Integer.parseInt(getWord(line, start + 3, ":"));
					int seconds = Integer.parseInt(getWord(line, start + 6, ":"));
					int milliseconds = Integer.parseInt(getWord(line, start + 9, " "));
					long addDate = new GregorianCalendar(2018, 11, 25, hours, minutes, seconds).getTimeInMillis()
							+ milliseconds;

					// If first value, set duration to 10 000
					long duration = (previousDate != 0) ? addDate - previousDate : 10_000;

					// Building the list of wanted request types
					List<String> requestTypes = Arrays.asList("READ", "INSERT", "UPDATE");

					// Request specs and add all
					while (start > 0) {
						start = line.indexOf("[", start) + 1;

						SpecRequest addSpecs = new SpecRequest(getWord(line, start, "]"));
						String addType = addSpecs.getType();
						int nbOps = addSpecs.getOpCount();
						double avg = addSpecs.getAvgLatency();

						if (requestTypes.contains(addType)) {

							/*
							 * Calculating the parameter of the latency distribution
							 */
							LogNormalFunc f = new LogNormalFunc(avg);
							double param = addSpecs.estimateParameter(f, addSpecs.getPercentile(0.9), 1E-15);

							LogNormalDistribution dist = new LogNormalDistribution(
									Math.log(avg) - Math.pow(param, 2) / 2., param);

							nbOps /= 1;

							/*
							 * Adding requests
							 */
							for (int op = 0; op < nbOps; op++) {
								validRequest.get(0).add(addDate + (long) (op * duration / nbOps));
								validRequest.get(1).add(addType);

								// double addLatency = dist.sample();
								validRequest.get(2).add(avg);
							}

						}

					}

					previousDate = addDate;

				}
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return validRequest;
	}

	/**
	 * Return the join of the two workloads. This method does not consider that
	 * values should be re-sorted</br>
	 * TODO change this method in an optimized way to re-sort values by timestamp
	 * and change the absolute timestamps
	 * 0=date(long), 1=type(String), 2=latency(double)
	 */
	public static final List<List<Object>> getAllRequestsFromFile(int nbNodes) {

		List<List<Object>> requestsW = getRequestsFromFile(nbNodes, loadMode.WRITE);
		List<List<Object>> requestsR = getRequestsFromFile(nbNodes, loadMode.READ);

		List<List<Object>> requestsMerged = new ArrayList<List<Object>>();
		for (int field = 0; field < requestsW.size(); field++) {
			List<Object> add = new ArrayList<>();
			add.addAll(requestsW.get(field));
			add.addAll(requestsR.get(field));
			requestsMerged.add(add);
		}

		return requestsMerged;

	}

	/**
	 * Returns all the substring of source between start and the index of separator.
	 * If separator is not contained in the string, then all the right side of the
	 * string is returned
	 */
	public static String getWord(String source, int start, String separator) {
		return source.substring(start,
				(source.substring(start).contains(separator)) ? source.indexOf(separator, start) : source.length());
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

		int[] vms = getMonitoringVmsList(nbNodes);

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
		if (componentId < 3 || componentId > 2 + nbDNs) {
			throw new IllegalArgumentException(
					"componentId must point to one of the datanodes (3 <= componentId <= 2+nbDNs)");
		}

		// TODO check typeData to avoid having less than two modes

		int[] vms = getMonitoringVmsList(nbDNs);

		// Calculating specified freqDist
		TreeMap<Double, Double> freqDist = getFrequencyDist(nbDNs, type, vms[componentId - 3], precision);
		// Getting sub distributions
		List<Map<Object, Double>> modesDists = getModesList(freqDist, precision);
		// Getting the right dist based on loadMode. TODO check that the first and last
		// modes are always the good ones
		switch (load) {
		case WRITE:
			return fitDistribution(modesDists.get(0));
		case READ:
			return fitDistribution(modesDists.get(modesDists.size() - 1));
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
	private static double[] fitDistribution(Map<Object, Double> dist) {
		WeightedObservedPoints data = new WeightedObservedPoints();
		int c = 0;
		for (double freq : dist.values()) {
			data.add(c++, freq);
		}

		List<Double> listKeyset = (List<Double>) (List<?>) (new ArrayList<>(dist.keySet()));
		double avg = dist.keySet().stream().mapToDouble(s -> (Double) s).average().getAsDouble();
		double std = (Collections.max(listKeyset) - Collections.min(listKeyset)) / 4.;

		return new double[] { avg, std };

	}

	/**
	 * Returns an array containing the identifiers of VMs depending on the number of
	 * nodes considered. This method is useful to read monitoring files from the
	 * YCSB workload</br>
	 * TODO modify this method depending on the identifiers of the VMs
	 * 
	 * @param nbNodes
	 * @return
	 */
	private static int[] getMonitoringVmsList(int nbNodes) {
		int[] vms;
		switch (nbNodes) {
		case 3:
			vms = new int[] { 111, 144, 164 };
			break;
		case 9:
			vms = new int[] { 111, 121, 122, 142, 143, 144, 164, 212, 250 }; // VM 149 not a data node
			break;
		default:
			throw new IllegalArgumentException("nbNodes can only be 3 or 9 using GenerateYCSBWorkload");
		}
		return vms;
	}

	/**
	 * Returns the contents of the specified monitoring file</br>
	 * The Date is NOT calculated from the original date of the file
	 * 0=relative timestamp (double), 1=absolute timestamp(Date), 2=value (double)
	 * 
	 * @param numberNodes 3 or 9
	 */
	private static List<List<Object>> readMonitoring(int numberNodes, typeData type, int vm) {

		String filepath = getFilePath(numberNodes, type, vm);

		List<List<Object>> columns = new ArrayList<List<Object>>();

		try {
			File file = new File(filepath);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String line = bufferedReader.readLine();

			// chnge the 3 t have more field columns in the returning object
			for (int field = 0; field < 3; field++) {
				columns.add(new ArrayList<Object>());
			}

			while ((line = bufferedReader.readLine()) != null) {
				double addRelTime = Double.parseDouble(getWord(line, 0, ","));
				Date addAbsTime = readTimeMonitoring(getWord(line, line.indexOf(",") + 1, ","));
				double addValue = Double.parseDouble(getWord(line, line.indexOf(",", line.indexOf(",") + 1) + 1, ","));

				columns.get(0).add(addRelTime);
				columns.get(1).add(addAbsTime);
				columns.get(2).add(addValue);
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return columns;

	}

	/**
	 * Reads the time in the format specified in monitoring files and returns the
	 * value as a {@link Date}
	 */
	private static Date readTimeMonitoring(String time) {

		int start = 0;
		int year = Integer.valueOf(getWord(time, start, "-"));
		start = time.indexOf("-", start) + 1;
		int month = Integer.valueOf(getWord(time, start, "-"));
		start = time.indexOf("-", start) + 1;
		int day = Integer.valueOf(getWord(time, start, "T"));
		start = 1 + time.indexOf("T");
		int hours = Integer.valueOf(getWord(time, start, ":"));
		start = 1 + time.indexOf(":", start);
		int minutes = Integer.valueOf(getWord(time, start, ":"));
		start = 1 + time.indexOf(":", start);
		int seconds = Integer.valueOf(getWord(time, start, "Z"));

		return new GregorianCalendar(year, month, day, hours, minutes, seconds).getTime();
	}

	/**
	 * Removes values that are out of the bounds of the workload and the extreme
	 * values</br>
	 * Complexity = getRequestsFromFile
	 * 
	 * @param nbNodes
	 * @param type
	 * @param writeOrRead
	 * @param vm
	 * @return
	 */
	private static List<List<Object>> cleanDataset(int nbNodes, typeData type, loadMode writeOrRead, int vm) {
		List<List<Object>> dataset = readMonitoring(nbNodes, type, vm);

		List<List<Object>> validRequest = getRequestsFromFile(nbNodes, writeOrRead);
		long startTime = (Long) validRequest.get(0).get(0);
		long endTime = (Long) validRequest.get(0).get(validRequest.get(0).size() - 1);

		// clean all values out of specified bounds
		int time = 0;
		while (time < dataset.get(1).size()) {
			long date = ((Date) dataset.get(1).get(time)).getTime();
			if (date <= startTime || date > endTime) {
				for (int field = 0; field < dataset.size(); field++) {
					dataset.get(field).remove(time);
				}
			} else {
				time++;
			}
		}

		return dataset;
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
	private static TreeMap<Double, Double> getFrequencyDist(int nbNodes, typeData type, int vm, double precision) {
		List<Double> dataset = (List<Double>) (List<?>) readMonitoring(nbNodes, type, vm).get(2);

		TreeMap<Double, Double> res = new TreeMap<Double, Double>();
		final int nbClasses = 1 + (int) (Collections.max(dataset) / precision);

		for (int i = 0; i <= nbClasses; i++) {
			res.put(precision * i, 0.);
		}

		for (double value : dataset) {
			double key = precision * (int) (value / precision);
			res.put(key, res.get(key) + 1);
		}

		for (double key : res.keySet()) {
			String s = "";
			for (int i = 0; i < res.get(key).intValue(); i++) {
				s += "o";
			}
			System.out.println(key + " " + s);
		}

		return res;
	}

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