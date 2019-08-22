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

import org.apache.commons.math3.distribution.LogNormalDistribution;

import Distribution.LogNormalFunc;

public class TxtReader {

	public static void main(String[] args) {
		histogramFrequencyDist(9, typeData.NetworkReceived, 122);
	}

	public enum typeData {
		CpuLoad, DiskIOReads, DiskIOWrites, MemoryUsage, NetworkReceived, NetworkSent
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
	 * 
	 * @throws InterruptedException
	 */
	public static double[] calculateRepartNodes(int nbNodes, typeData type) {

		int[] vms = getMonitoringVmsList(nbNodes);

		// getting values
		List<List<Double>> cpuLoads = getValues(nbNodes, type, vms);

		// calculating normalized values
		List<List<Double>> normCpuLoads = new ArrayList<List<Double>>();

		for (int vm = 0; vm < cpuLoads.size(); vm++) {

			List<Double> add = new ArrayList<Double>();

			for (int time = 0; time < cpuLoads.get(vm).size(); time++) {
				double sum = 0;
				for (int vm_bis = 0; vm_bis < cpuLoads.size(); vm_bis++) {
					sum += cpuLoads.get(vm_bis).get(time);
				}

				add.add(cpuLoads.get(vm).get(time) * ((sum == 0) ? 1 : 1 / sum));
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
	 * return a double[] of size nbNodes (number of VMs) containing the average
	 * values of each VM calculated from the specified monitoring file
	 * 
	 * @param nbNodes
	 * @param type
	 * @return
	 */
	public static double[] getAvgValues(int nbNodes, typeData type) {

		int[] vms = getMonitoringVmsList(nbNodes);

		// getting cpuLoads
		List<List<Double>> cpuLoads = getValues(nbNodes, type, vms);

		double[] avgs = new double[vms.length];
		for (int vm = 0; vm < vms.length; vm++) {
			for (int time = 0; time < cpuLoads.get(vm).size(); time++) {
				avgs[vm] += cpuLoads.get(vm).get(time);
			}
			avgs[vm] /= (double) cpuLoads.get(vm).size();
		}

		return avgs;

	}

	public static double[] getStdValues(int nbNodes, typeData type) {

		int[] vms = getMonitoringVmsList(nbNodes);

		List<List<Double>> values = getValues(nbNodes, type, vms);

		double[] stds = new double[vms.length];
		for (int vm = 0; vm < vms.length; vm++) {
			stds[vm] = (Collections.max(values.get(vm)) - Collections.min(values.get(vm))) / 4.;
		}

		return stds;
	}

	/**
	 * 0=date, 1=type, 2=latency</br>
	 * writeOrRead = "W" or "R"
	 */
	public static List<List<Object>> getRequestsFromFile(int nbNodes, String writeOrRead) {

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
		case "W":
			fileName = "load.txt";
			break;
		case "R":
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

		List<List<Object>> requestsW = getRequestsFromFile(nbNodes, "W");
		List<List<Object>> requestsR = getRequestsFromFile(nbNodes, "R");

		List<List<Object>> requestsMerged = new ArrayList<List<Object>>();
		for (int field = 0; field < requestsW.size(); field++) {
			List<Object> add = new ArrayList<>();
			add.addAll(requestsW.get(field));
			add.addAll(requestsR.get(field));
			requestsMerged.add(add);
		}

		return requestsMerged;

	}

	private static List<List<Double>> getValues(int nbNodes, typeData type, int[] vmList) {
		// getting cpuLoads
		List<List<Double>> values = new ArrayList<>();
		for (int vm : vmList) {
			@SuppressWarnings("unchecked")
			List<Double> add = (List<Double>) (List<?>) readMonitoring(nbNodes, type, vm).get(2);

			if (vm == 111)
				add = add.subList(10, add.size()); // removing the 10 first values of VM111 to align timestamps
			values.add(add);
		}

		return values;
	}

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
		String path = "/elasticsearch_nodes-" + numberNodes + "_replication-3/nodes-" + numberNodes
				+ "_replication-3/evaluation_run_2018_11_25-";
		if (numberNodes == 3)
			path += "19_10/monitoring/";
		if (numberNodes == 9)
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

		List<List<Object>> columns = new ArrayList<List<Object>>();

		try {
			File file = new File(System.getProperty("user.dir") + File.separator + path + File.separator + fileName);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String line = bufferedReader.readLine();

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
	 * Returns all the substring of source between start and the index of separator.
	 * If separator is not contained in the string, then all the right side of the
	 * string is returned
	 */
	public static String getWord(String source, int start, String separator) {
		return source.substring(start,
				(source.substring(start).contains(separator)) ? source.indexOf(separator, start) : source.length());
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
	 * Calculates and returns the time in seconds from the textual format used in
	 * the data txt files
	 */
	private static long readTimeWorkload(String time) {
		int index = 0;
		long res = 0;
		int mult = 0;
		while (index < time.length()) {
			String num = getWord(time, index, " ");
			index += num.length() + 1;
			String hor = getWord(time, index, " ");
			index += hor.length() + 1;

			if (hor.contains("day")) {
				mult = 86_400;
			} else if (hor.contains("hour")) {
				mult = 3600;
			} else if (hor.contains("minute")) {
				mult = 60;
			} else if (hor.contains("second")) {
				mult = 1;
			}

			res += Long.parseLong(num) * mult;
		}
		return res;
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
	 * 0=write mode, 1=read mode
	 * 
	 * @param type
	 * @param vm
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static double[] getAvgValuesVM(int nbNodes, typeData type, int vm) {
		List<Double> valuesW = (List<Double>) (List<?>) cleanDataset(nbNodes, type, "W", vm).get(2);
		List<Double> valuesR = (List<Double>) (List<?>) cleanDataset(nbNodes, type, "R", vm).get(2);

		System.out.println(valuesW.toString());
		System.out.println(valuesR.toString());
		List<Double> values = (List<Double>) (List<?>) readMonitoring(nbNodes, type, vm).get(2);
		System.out.println(values.toString());
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new double[] { valuesW.stream().mapToDouble(s -> s).average().getAsDouble(),
				valuesR.stream().mapToDouble(s -> s).average().getAsDouble() };

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
	private static List<List<Object>> cleanDataset(int nbNodes, typeData type, String writeOrRead, int vm) {
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
	 * Plots the histogram of the specified data, the precision must be set
	 * 
	 * @param nbNodes
	 * @param type
	 * @param vm
	 */
	private static void histogramFrequencyDist(int nbNodes, typeData type, int vm) {
		List<Double> dataset = (List<Double>) (List<?>) readMonitoring(nbNodes, type, vm).get(2);

		// largeur des classes
		final double precision = 0.5;
		final int nbClasses = 1 + (int) (Collections.max(dataset).intValue() / precision);
		int[] nbOccurences = new int[nbClasses];

		for (double value : dataset) {
			nbOccurences[(int) (value / precision)] += 1;
		}

		for (int oc : nbOccurences) {
			String s = "";
			for (int i = 0; i < oc; i++) {
				s += "o";
			}
			System.out.println(s);
		}
	}

}