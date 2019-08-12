package Classes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;

import Distribution.GammaFunc;
import Distribution.LogNormalFunc;

@SuppressWarnings("unused")
public class TxtReader {

	public static void main(String[] args) throws InterruptedException {
		long startTime = System.currentTimeMillis();

		List<List<Object>> aaa = mergeWorkloads2();

		for (Object i : aaa.get(0)) {
			System.out.println((Long) i - (Long) aaa.get(0).get(0));
		}

		System.out.println("size:" + aaa.get(0).size());
		System.out.println("time:" + (System.currentTimeMillis() - startTime) / 1000.);
	}

	/**
	 * Calculates the average weights of operations of each type, based on the
	 * average latency of requests of each type</br>
	 * These weights can help us calculate MI necessary to execute requests
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Double> calculateCyclesType() {

		List<SpecRequest> specs = (List<SpecRequest>) (List<?>) TxtReader.mergeWorkloads().get(5);

		Map<String, Double> tam = new HashMap<String, Double>();
		Map<String, Integer> sizes = new HashMap<String, Integer>();

		for (SpecRequest spec : specs) {
			String type = spec.getType();
			double avgLat = spec.getAvgLatency();

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
	 * nodes, based on CpuLoad
	 */
	@SuppressWarnings("unchecked")
	public static Double[] calculateRepartNodes(int nbNodes, typeData type) {

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

		// getting cpuLoads
		List<List<Double>> cpuLoads = new ArrayList<List<Double>>();
		for (int vm : vms) {
			List<Double> add = (List<Double>) (List<?>) readMonitoring(nbNodes, type, vm).get(2);

			if (vm == 111)
				add = add.subList(10, add.size()); // removing the 10 first values of VM111 to align timestamps
			cpuLoads.add(add);
		}

		// calculating normalized cpu load
		List<List<Double>> normCpuLoads = new ArrayList<List<Double>>();

		for (int vm = 0; vm < cpuLoads.size(); vm++) {

			List<Double> add = new ArrayList<Double>();

			for (int time = 0; time < cpuLoads.get(vm).size(); time++) {
				double sum = 0;
				for (int vm_bis = 0; vm_bis < cpuLoads.size(); vm_bis++) {
					sum += cpuLoads.get(vm_bis).get(time);
				}

				add.add(cpuLoads.get(vm).get(time) * ((sum != 0) ? 1 / sum : 1));
			}
			normCpuLoads.add(add);
		}

		// average for each VM of the normalized cpu load
		Double[] distribution = new Double[vms.length];

		for (int vm = 0; vm < normCpuLoads.size(); vm++) {
			double sum = 0;
			for (int time = 0; time < normCpuLoads.get(vm).size(); time++) {
				sum += normCpuLoads.get(vm).get(time);
			}

			distribution[vm] = sum / normCpuLoads.get(vm).size();
		}

		return distribution;

	}

	public static int[][] initFromSource(String filepath, String choice) {
		try {
			FileReader fr = new FileReader(new File(filepath));
			BufferedReader br = new BufferedReader(fr);

			String line = br.readLine();
			int NB_SITES = Integer.valueOf(getWord(line, line.indexOf(" = ") + 3, "?"));
			line = br.readLine();
			int NB_NODES = Integer.valueOf(getWord(line, line.indexOf(" = ") + 3, "?"));

			int[][] init = new int[NB_SITES][NB_NODES];

			int i = 0;
			int j = 0;
			while ((line = br.readLine()) != null) {

				if (line.startsWith("nSite"))
					i = Integer.valueOf(getWord(line, line.indexOf(" = ") + 3, "?"));

				if (line.startsWith("nNode"))
					j = Integer.valueOf(getWord(line, line.indexOf(" = ") + 3, "?"));

				if (line.startsWith(choice))
					init[i][j] = Integer.valueOf(getWord(line, line.indexOf(" = ") + 3, "?"));
			}

			br.close();

			return init;

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}

	public static void generateSource(int NB_SITES, int NB_NODES) {
		try {
			File file = new File("C:/Users/josf9/git/Test/source_init");
			FileWriter filewriter = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(filewriter);

			bw.write("NB_SITES = " + NB_SITES + "\n" + "NB_NODES = " + NB_NODES + "\n\n");

			for (int site = 0; site < NB_SITES; site++) {
				bw.write("nSite = " + site + "\n\n");
				for (int node = 0; node < NB_NODES; node++) {
					bw.write("nNode = " + node + "\n");
					bw.write("cpuFrequency = " + "\n");
					bw.write("cpuNodes = " + "\n");
					bw.write("ram = " + "\n");
					bw.write("hdd = " + "\n\n");
				}
			}

			bw.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	public static List<List<Object>> readMonitoring(int numberNodes, typeData type, int vm) {
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
				columns.get(0).add(Double.parseDouble(getWord(line, 0, ",")));
				columns.get(1).add(readTimeMonitoring(getWord(line, line.indexOf(",") + 1, ",")));
				columns.get(2)
						.add(Double.parseDouble(getWord(line, line.indexOf(",", line.indexOf(",") + 1) + 1, ",")));
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return columns;

	}

	/**
	 * 0=date, 1=type, 2=latency</br>
	 */
	@SuppressWarnings("deprecation")
	public static List<List<Object>> getRequests2(int numberNodes, writeOrRead pick) {
		String path = "/elasticsearch_nodes-" + numberNodes + "_replication-3/nodes-" + numberNodes
				+ "_replication-3/evaluation_run_2018_11_25-";
		if (numberNodes == 3)
			path += "19_10/data/";
		if (numberNodes == 9)
			path += "22_40/data/";

		String fileName = "";
		switch (pick) {
		case W:
			fileName = "load.txt";
			break;
		case R:
			fileName = "transaction.txt";
		}

		String line = null;

		List<List<Object>> validRequest = new ArrayList<List<Object>>();

		final int NB_FIELDS = 6;
		for (int field = 0; field < NB_FIELDS; field++) {
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
					long addDate = milliseconds + 1000 * seconds + 1000 * 60 * minutes + 1000 * 60 * 60 * hours
							+ new Date(2018, 11, 25).getTime();

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

						GammaFunc f = new GammaFunc(avg);
						//LogNormalFunc f = new LogNormalFunc(avg);
						double param = addSpecs.estimateParameter(f, addSpecs.getPercentile(90), 10);

						GammaDistribution dist = new GammaDistribution(param, param / avg);
						//LogNormalDistribution dist = new LogNormalDistribution(Math.log(avg) - Math.pow(param, 2) / 2,
							//	param);

						/*
						 * The processing time rises fast with the number of requests, we don't need 2
						 * million requests so we only take a proportion TODO : optimisation to compare
						 * full workloads
						 */
						nbOps /= 100;

						double totalLatency = 0;
						int countLatency = 0;

						if (requestTypes.contains(addType)) {
							
							System.out.println("-----------------------------------");
							System.out.println("AVERAGE:" + avg);
							
							for (int op = 0; op < nbOps; op++) {
								validRequest.get(0).add(addDate + (long) (duration * op / nbOps));
								validRequest.get(1).add(addType);

								double addLatency = dist.sample();
								validRequest.get(2).add(dist.sample());
								totalLatency += addLatency;
								countLatency += 1;
								
								System.out.println("Latency:"+addLatency);
							}
							
							System.out.println("calculated avg="+totalLatency/countLatency);
							Thread.sleep(100);
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
	 * 0=date(long), 1=type(String), 2=latency(double)
	 */
	@SuppressWarnings("unchecked")
	public static final List<List<Object>> mergeWorkloads2() {
		List<List<Object>> workloadR = getRequests2(9, writeOrRead.R);
		List<List<Object>> workloadW = getRequests2(9, writeOrRead.W);

		List<List<Object>> workloadMerged = new ArrayList<List<Object>>();
		for (int field = 0; field < 3; field++) {
			workloadMerged.add(new ArrayList<>());
		}

		int sizeMerged = workloadR.get(0).size() + workloadW.get(0).size();

		while (workloadMerged.get(0).size() < sizeMerged) {

			long minR = Collections.min((List<Long>) (List<?>) workloadR.get(0));
			long minW = Collections.min((List<Long>) (List<?>) workloadW.get(0));
			long min = Math.min(minR, minW);

			List<List<Object>> workloadMin = (min == minR) ? workloadR : workloadW;
			int indexMin = workloadMin.get(0).indexOf(min);

			for (int field = 0; field < 3; field++) {
				workloadMerged.get(field).add(workloadMin.get(field).get(indexMin));
			}

			((min == minR) ? workloadR : workloadW).get(0).set(indexMin, Long.MAX_VALUE);

		}

		return workloadMerged;

	}

	/**
	 * 0=Date, 1=time, 2=nOp, 3=throughput, 4=estTime, 5=specs
	 */
	@SuppressWarnings("deprecation")
	public static List<List<Object>> getRequests(int numberNodes, writeOrRead pick) {
		String path = "/elasticsearch_nodes-" + numberNodes + "_replication-3/nodes-" + numberNodes
				+ "_replication-3/evaluation_run_2018_11_25-";
		if (numberNodes == 3)
			path += "19_10/data/";
		if (numberNodes == 9)
			path += "22_40/data/";

		String fileName = "";
		switch (pick) {
		case W:
			fileName = "load.txt";
			break;
		case R:
			fileName = "transaction.txt";
		}

		String line = null;
		List<List<Object>> validRequest = new ArrayList<List<Object>>();

		try {
			File file = new File(System.getProperty("user.dir") + File.separator + path + File.separator + fileName);

			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			final int NB_FIELDS = 6;

			for (int field = 0; field < NB_FIELDS; field++) {
				validRequest.add(new ArrayList<Object>());
			}

			while ((line = bufferedReader.readLine()) != null) {

				if (line.startsWith("2018") && !line.contains("Thread")) {

					// Date
					int start = 11;
					int hours = Integer.parseInt(getWord(line, start, ":"));
					int minutes = Integer.parseInt(getWord(line, start + 3, ":"));
					int seconds = Integer.parseInt(getWord(line, start + 6, ":"));
					int milliseconds = Integer.parseInt(getWord(line, start + 9, " "));
					long addDate = milliseconds + 1000 * seconds + 1000 * 60 * minutes + 1000 * 60 * 60 * hours
							+ new Date(2018, 11, 25).getTime();

					// Timestamp
					start = 24;
					int addTime = Integer.parseInt(getWord(line, start, " "));

					// Number of operations
					start = line.indexOf("sec:", start) + 5;
					int addNOp = Integer.parseInt(getWord(line, start, " "));

					// Throughput
					double addThroughput = Double.MAX_VALUE;
					if (line.contains("ops/sec")) {
						start = line.indexOf("operations;", start) + 12;
						addThroughput = Double.parseDouble(getWord(line, start, " "));
					}

					// Estimate time of completion
					long addEstTime = 0;
					if (line.contains("est completion in")) {
						start = line.indexOf("est completion in", start) + 18;
						addEstTime = readTimeWorkload(getWord(line, start, "["));
					}

					// Building the list of wanted request types
					List<String> requestTypes = Arrays.asList("READ", "INSERT", "UPDATE");

					// Request specs and add all
					if (line.contains("[")) {
						start = line.indexOf("[", start) + 1;
						while (start > 0) {
							SpecRequest addSpecs = new SpecRequest(getWord(line, start, "]"));

							if (requestTypes.contains(addSpecs.getType())) {
								validRequest.get(0).add(addDate);
								validRequest.get(1).add(addTime);
								validRequest.get(2).add(addNOp);
								validRequest.get(3).add(addThroughput);
								validRequest.get(4).add(addEstTime);
								validRequest.get(5).add(addSpecs);
							}
							start = line.indexOf("[", start) + 1;
						}
					}

				}
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return validRequest;
	}

	/**
	 * 0=Date, 1=time, 2=nOp, 3=throughput, 4=estTime, 5=Spec </br>
	 * Merges the workloads of load and transaction for the 9 nodes case, also sorts
	 * all operations according to their date
	 */
	@SuppressWarnings({ "unchecked" })
	public final static List<List<Object>> mergeWorkloads() {
		/*
		 * Sorting requests from both workloads
		 */
		List<List<Object>> validRequestsW = getRequests(9, writeOrRead.W);
		List<List<Object>> validRequestsR = getRequests(9, writeOrRead.R);

		List<Long> vRWdates = (List<Long>) (List<?>) validRequestsW.get(0);
		List<Long> vRRdates = (List<Long>) (List<?>) validRequestsR.get(0);

		List<List<Object>> validRequests = new ArrayList<List<Object>>();
		for (int field = 0; field < 6; field++) {
			validRequests.add(new ArrayList<Object>());
		}

		int indexWmin;
		int indexRmin;
		int indexMin;
		List<List<Object>> vRmin;
		for (int index = 0; index < vRWdates.size() + vRRdates.size(); index++) {
			indexWmin = vRWdates.indexOf(Collections.min(vRWdates));
			indexRmin = vRRdates.indexOf(Collections.min(vRRdates));

			if (vRWdates.get(indexWmin) >= vRRdates.get(indexRmin)) {
				indexMin = indexRmin;
				vRmin = validRequestsR;
			} else {
				indexMin = indexWmin;
				vRmin = validRequestsW;
			}

			for (int field = 0; field < 6; field++) {
				validRequests.get(field).add(vRmin.get(field).get(indexMin));
			}

			if (vRmin.equals(validRequestsW)) {
				vRWdates.set(indexWmin, Long.MAX_VALUE);
			} else {
				vRRdates.set(indexRmin, Long.MAX_VALUE);
			}

		}

		return validRequests;
	}

	public enum writeOrRead {
		W, R
	}

	public enum typeData {
		CpuLoad, DiskIOReads, DiskIOWrites, MemoryUsage, NetworkReceived, NetworkSent
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////// OTHER METHODS AND CLASSES
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Returns all the substring of source between start and the index of separator.
	 * If separator is not contained in the string, then all the right side of the
	 * string is returned
	 */
	public static String getWord(String source, int start, String separator) {
		return source.substring(start,
				(source.substring(start).contains(separator)) ? source.indexOf(separator, start) : source.length());
	}

	@SuppressWarnings("deprecation")
	public static Date readTimeMonitoring(String time) {
		Date date = new Date();

		int start = 1 + time.indexOf("T");
		date.setHours(Integer.valueOf(getWord(time, start, ":")));

		start = 1 + time.indexOf(":", start);
		date.setMinutes(Integer.valueOf(getWord(time, start, ":")));

		start = 1 + time.indexOf(":", start);
		date.setSeconds(Integer.valueOf(getWord(time, start, "Z")));

		return date;
	}

	/**
	 * Calculates and returns the time in seconds from the textual format used in
	 * the data txt files
	 */
	public static long readTimeWorkload(String time) {
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

}