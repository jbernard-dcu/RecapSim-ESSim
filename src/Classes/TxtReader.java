package Classes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TxtReader {

	@SuppressWarnings({ "deprecation", "unchecked" })
	public static List<List<Object>> mergeWorkloads() {
		/*
		 * Sorting requests from both workloads
		 */
		List<List<Object>> vRW = getRequests(9, writeOrRead.W);
		List<List<Object>> vRR = getRequests(9, writeOrRead.R);

		List<Date> vRWdates = (List<Date>) (List<?>) vRW.get(0);
		List<Date> vRRdates = (List<Date>) (List<?>) vRR.get(0);

		List<List<Object>> vR = new ArrayList<List<Object>>();
		for (int field = 0; field < 6; field++) {
			vR.add(new ArrayList<Object>());
		}

		int indexWmin;
		int indexRmin;
		int indexMin;
		List<List<Object>> vRmin;
		for (int index = 0; index < vRWdates.size() + vRRdates.size(); index++) {
			indexWmin = vRWdates.indexOf(Collections.min(vRWdates));
			indexRmin = vRRdates.indexOf(Collections.min(vRRdates));

			if (vRWdates.get(indexWmin).getTime() >= vRRdates.get(indexRmin).getTime()) {
				indexMin = indexRmin;
				vRmin = vRR;
			} else {
				indexMin = indexWmin;
				vRmin = vRW;
			}

			for (int field = 0; field < 6; field++) {
				vR.get(field).add(vRmin.get(field).get(indexMin));
			}

			if (vRmin.equals(vRW)) {
				vRWdates.set(indexWmin, new Date(2020, 1, 1));
			} else {
				vRRdates.set(indexRmin, new Date(2020, 1, 1));
			}

		}

		return vR;
	}

	/**
	 * Method to calculate the approx. repartition of the requests among the data
	 * nodes, based on CpuLoad
	 */
	@SuppressWarnings("unchecked")
	public static double[] calculateRepart(int nbNodes) {

		int[] vms = {};
		switch (nbNodes) {
		case 3:
			vms = new int[] { 111, 144, 164 };
			break;
		case 9:
			vms = new int[] { 111, 121, 122, 142, 143, 144, 164, 212, 250 }; // VM 149 not a data node
			break;
		default:
			throw new IllegalArgumentException("nbNodes can only be 3 or 9");
		}

		// getting cpuLoads
		List<List<Double>> cpuLoads = new ArrayList<List<Double>>();
		for (int vm : vms) {
			List<Double> add = (List<Double>) (List<?>) readMonitoring(nbNodes, typeData.CpuLoad, vm).get(2);
			if (vm == 111)
				add = add.subList(10, add.size()); // removing the 10 first values of VM111 to fit timestamps
			cpuLoads.add(add);
		}

		// calculating normalized cpu load
		List<List<Double>> normCpuLoads = new ArrayList<List<Double>>();

		for (int field = 0; field < cpuLoads.size(); field++) {

			List<Double> add = new ArrayList<Double>();

			for (int time = 0; time < cpuLoads.get(field).size(); time++) {
				double sum = 0;
				for (int field2 = 0; field2 < cpuLoads.size(); field2++) {
					sum += cpuLoads.get(field2).get(time);
				}
				add.add(cpuLoads.get(field).get(time) / sum);
			}
			normCpuLoads.add(add);
		}

		// average for each VM of the normalized cpu laod
		double[] distribution = new double[vms.length];

		for (int field = 0; field < normCpuLoads.size(); field++) {
			double sum = 0;
			for (int time = 0; time < normCpuLoads.get(field).size(); time++) {
				sum += normCpuLoads.get(field).get(time);
			}

			distribution[field] = sum / normCpuLoads.get(field).size();
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

	public enum typeData {
		CpuLoad, DiskIOReads, DiskIOWrites, MemoryUsage, NetworkReceived, NetworkSent
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

			final int NB_FIELDS = 3;
			for (int field = 0; field < NB_FIELDS; field++) {
				columns.add(new ArrayList<Object>());
			}

			while ((line = bufferedReader.readLine()) != null) {
				columns.get(0).add(Double.parseDouble(getWord(line, 0, ",")));
				columns.get(1).add(readTime2(getWord(line, line.indexOf(",") + 1, ",")));
				columns.get(2)
						.add(Double.parseDouble(getWord(line, line.indexOf(",", line.indexOf(",") + 1) + 1, ",")));
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return columns;

	}

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
					@SuppressWarnings("deprecation")
					Date addDate = new Date(milliseconds + 1000 * seconds + 1000 * 60 * minutes + 1000 * 60 * 60 * hours
							+ new Date(2018, 11, 25).getTime());

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
						addEstTime = readTime(getWord(line, start, "["));
					}

					// Request type and add all
					if (!line.contains("[")) {
						validRequest.get(0).add(addDate);
						validRequest.get(1).add(addTime);
						validRequest.get(2).add(addNOp);
						validRequest.get(3).add(addThroughput);
						validRequest.get(4).add(addEstTime);
						validRequest.get(5).add(new SpecRequest());
					} else {
						start = line.indexOf("[", start) + 1;
						// TODO fix
						while (start > 0) {
							validRequest.get(0).add(addDate);
							validRequest.get(1).add(addTime);
							validRequest.get(2).add(addNOp);
							validRequest.get(3).add(addThroughput);
							validRequest.get(4).add(addEstTime);
							validRequest.get(5).add(new SpecRequest(getWord(line, start, "]")));
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

	public enum writeOrRead {
		W, R
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
	public static Date readTime2(String time) {
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
	public static long readTime(String time) {
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

/**
 * Class modeling the specifications of a request, as given in the workload data
 */
class SpecRequest {
	private String type;
	private int count;
	private int max;
	private int min;
	private double avg;
	private int q90;
	private int q99;
	private int q999;
	private int q9999;

	public SpecRequest() {
		this.type = "NONE";
		this.count = 0;
		this.max = 0;
		this.min = 0;
		this.avg = 0;
		this.q90 = 0;
		this.q99 = 0;
		this.q999 = 0;
		this.q9999 = 0;
	}

	public SpecRequest(String init) {
		this.type = TxtReader.getWord(init, 0, ": ");
		this.count = Integer.parseInt(TxtReader.getWord(init, init.indexOf("Count=") + 6, ", "));
		this.max = Integer.parseInt(TxtReader.getWord(init, init.indexOf("Max=") + 4, ", "));
		this.min = Integer.parseInt(TxtReader.getWord(init, init.indexOf("Min=") + 4, ", "));
		this.avg = Double.parseDouble(TxtReader.getWord(init, init.indexOf("Avg=") + 4, ", "));
		this.q90 = Integer.parseInt(TxtReader.getWord(init, init.indexOf("90=") + 3, ", "));
		this.q99 = Integer.parseInt(TxtReader.getWord(init, init.indexOf("99=") + 3, ", "));
		this.q999 = Integer.parseInt(TxtReader.getWord(init, init.indexOf("99.9=") + 5, ", "));
		this.q9999 = Integer.parseInt(init.substring(init.indexOf("99.99=") + 6));
	}

	public String getType() {
		return this.type;
	}

	public String toString() {
		List<Object> list = new ArrayList<Object>();
		list.add(type);
		list.add(count);
		list.add(max);
		list.add(min);
		list.add(avg);
		list.add(q90);
		list.add(q99);
		list.add(q999);
		list.add(q9999);
		return list.toString() + "\n";
	}

	/**
	 * Returns the average expected time in milliseconds to complete the request,
	 * based on nb_operations and throughput
	 */
	public double getAvgTime() {
		return this.avg;
	}
}
