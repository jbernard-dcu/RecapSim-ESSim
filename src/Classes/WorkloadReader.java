package Classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.LogNormalDistribution;

import Classes.ReaderUtils.loadMode;
import Distribution.LogNormalFunc;

public class WorkloadReader {

	private loadMode mode;
	private List<List<Object>> data;

	public WorkloadReader(int nbNodes, loadMode mode) {
		this.mode = mode;

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
		switch (mode) {
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
					int start = 1;
					String stringDate = line.substring(0, 23);
					long addDate = ReaderUtils.readTimeWorkload(stringDate);

					// If first value, set duration to 10 000
					long duration = (previousDate != 0) ? addDate - previousDate : 10_000;

					// Building the list of wanted request types
					List<String> requestTypes = Arrays.asList("READ", "INSERT", "UPDATE");

					// Request specs and add all
					while (start > 0) {
						start = line.indexOf("[", start) + 1;

						SpecRequest addSpecs = new SpecRequest(ReaderUtils.getWord(line, start, "]"));
						String addType = addSpecs.getType();
						int nbOps = addSpecs.getOpCount();
						double avg = addSpecs.getAvgLatency();

						if (requestTypes.contains(addType)) {
							/*
							 * // Calculating the parameter of the latency distribution
							 * LogNormalFunc f = new LogNormalFunc(avg);
							 * double param = addSpecs.estimateParameter(f, addSpecs.getPercentile(0.9),
							 * 1E-15);
							 * 
							 * LogNormalDistribution dist = new LogNormalDistribution(
							 * Math.log(avg) - Math.pow(param, 2) / 2., param);
							 */
							nbOps /= 1;
							// Adding requests
							for (int op = 0; op < nbOps; op++) {
								validRequest.get(0).add(addDate + (long) (op * duration / nbOps));
								validRequest.get(1).add(addType);

								// double addLatency = dist.sample();
								validRequest.get(2).add(avg);
							}

							// validRequest.get(0).add(addDate);
							// validRequest.get(1).add(addType);
							// validRequest.get(2).add(avg);

						}

					}

					previousDate = addDate;

				}
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		this.data = validRequest;

	}

	public List<List<Object>> getData() {
		return this.data;
	}

	public loadMode getMode() {
		return this.mode;
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
