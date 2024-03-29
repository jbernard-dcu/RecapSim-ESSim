package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.LogNormalDistribution;

import distribution.LogNormalFunc;
import utils.TxtUtils.loadMode;

public class WorkloadReader {

	private loadMode mode;
	private List<List<Object>> data;

	private WorkloadReader(int nbNodes, loadMode mode, int nbRequest) {
		this.mode = mode;

		// Checking that nbRequest is positive
		if (nbRequest <= 0)
			nbRequest = Integer.MAX_VALUE;

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
			throw new IllegalArgumentException("nbNodes cas only be 3 or 9");
		}

		// Checking that mode is equal to "W" or "R"
		String fileName = "";
		switch (mode) {
		case WRITE:
			fileName = "load.txt";
			break;
		case READ:
			fileName = "transaction.txt";
			break;
		default:
			throw new IllegalArgumentException("loadMode can only be WRITE or READ");
		}

		String line = null;

		List<List<Object>> validRequest = new ArrayList<>();
		for (int field = 0; field < 3; field++) {
			validRequest.add(new ArrayList<>());
		}

		try {
			File file = new File(System.getProperty("user.dir") + File.separator + path + File.separator + fileName);

			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			long previousDate = 0;

			// Building the list of wanted request types
			List<String> requestTypes = Arrays.asList("READ", "INSERT", "UPDATE");

			while ((line = bufferedReader.readLine()) != null && validRequest.get(0).size() < nbRequest) {

				if (line.startsWith("2018") && !line.contains("Thread") && line.contains("[")) {

					// Date
					String stringDate = line.substring(0, 23);
					long addDate = TxtUtils.readTimeWorkload(stringDate);

					// If first value, set duration to 10 000
					long duration = (previousDate != 0) ? addDate - previousDate : 10_000;

					// Request specs and add all
					int start = 1;
					while (start > 0 && validRequest.get(0).size() < nbRequest) {
						start = line.indexOf("[", start) + 1;

						SpecRequest addSpecs = new SpecRequest(TxtUtils.getWord(line, start, "]"));
						String addType = addSpecs.getType();
						int nbOps = addSpecs.getOpCount();
						double avg = addSpecs.getAvgLatency();

						if (requestTypes.contains(addType)) {

							// Calculating the parameter of the latency distribution
							LogNormalFunc f = new LogNormalFunc(avg);
							double param = addSpecs.estimateParameter(f, addSpecs.getPercentile(0.9), 1E-15);

							LogNormalDistribution dist = new LogNormalDistribution(
									Math.log(avg) - Math.pow(param, 2) / 2., param);

							/*
							 * // reduce the number of requests generation per operation, ie size of dataset
							 * nbOps /= 1000;
							 * // Adding requests
							 * for (int op = 0; op < nbOps; op++) {
							 * validRequest.get(0).add(addDate + (long) (op * duration / nbOps));
							 * validRequest.get(1).add(addType);
							 *
							 * double addLatency = dist.sample();
							 * validRequest.get(2).add(addLatency);
							 * }
							 */

							// adding only one request per workload line
							validRequest.get(0).add(addDate);
							validRequest.get(1).add(addType);
							validRequest.get(2).add(dist.sample());

						}

					}

					previousDate = addDate;

				}

				// Getting the output statistics and the GC information
				if (line.startsWith("[") && !line.startsWith("[2")) {

				}
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		this.data = validRequest;

	}

	public static WorkloadReader create(int nbNodes, loadMode mode, int nbRequest) {
		return new WorkloadReader(nbNodes, mode, nbRequest);
	}

	public List<List<Object>> getData() {
		return this.data;
	}

	public loadMode getMode() {
		return this.mode;
	}

	public int getNbRequests() {
		return data.get(0).size();
	}

	/**
	 * Calculates the average weights of operations of each type, based on the
	 * average latency of requests of each type</br>
	 * These weights can help us calculate MI necessary to execute requests
	 */
	public Map<String, Double> calculateCyclesType() {

		Map<String, Double> tam = new HashMap<String, Double>();
		Map<String, Integer> sizes = new HashMap<String, Integer>();

		for (int time = 0; time < data.get(0).size(); time++) {
			String type = (String) data.get(1).get(time);
			double avgLat = (double) data.get(2).get(time);

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

}
