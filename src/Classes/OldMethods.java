package Classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class OldMethods {
	
	/**
	 * 0=Date, 1=time, 2=nOp, 3=throughput, 4=estTime, 5=specs
	 */
	public static List<List<Object>> getRequests(int numberNodes, String pick) {
		String path = "/elasticsearch_nodes-" + numberNodes + "_replication-3/nodes-" + numberNodes
				+ "_replication-3/evaluation_run_2018_11_25-";
		if (numberNodes == 3)
			path += "19_10/data/";
		if (numberNodes == 9)
			path += "22_40/data/";

		String fileName = "";
		switch (pick) {
		case "W":
			fileName = "load.txt";
			break;
		case "R":
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
					int hours = Integer.parseInt(TxtReader.getWord(line, start, ":"));
					int minutes = Integer.parseInt(TxtReader.getWord(line, start + 3, ":"));
					int seconds = Integer.parseInt(TxtReader.getWord(line, start + 6, ":"));
					int milliseconds = Integer.parseInt(TxtReader.getWord(line, start + 9, " "));
					long addDate = milliseconds + 1000 * seconds + 1000 * 60 * minutes + 1000 * 60 * 60 * hours
							+ new Date(2018, 11, 25).getTime();

					// Timestamp
					start = 24;
					int addTime = Integer.parseInt(TxtReader.getWord(line, start, " "));

					// Number of operations
					start = line.indexOf("sec:", start) + 5;
					int addNOp = Integer.parseInt(TxtReader.getWord(line, start, " "));

					// Throughput
					double addThroughput = Double.MAX_VALUE;
					if (line.contains("ops/sec")) {
						start = line.indexOf("operations;", start) + 12;
						addThroughput = Double.parseDouble(TxtReader.getWord(line, start, " "));
					}

					// Estimate time of completion
					long addEstTime = 0;
					if (line.contains("est completion in")) {
						start = line.indexOf("est completion in", start) + 18;
						addEstTime = TxtReader.readTimeWorkload(TxtReader.getWord(line, start, "["));
					}

					// Building the list of wanted request types
					List<String> requestTypes = Arrays.asList("READ", "INSERT", "UPDATE");

					// Request specs and add all
					if (line.contains("[")) {
						start = line.indexOf("[", start) + 1;
						while (start > 0) {
							SpecRequest addSpecs = new SpecRequest(TxtReader.getWord(line, start, "]"));

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
	public final static List<List<Object>> mergeWorkloads() {
		/*
		 * Sorting requests from both workloads
		 */
		List<List<Object>> validRequestsW = getRequests(9, "W");
		List<List<Object>> validRequestsR = getRequests(9, "R");

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

}
