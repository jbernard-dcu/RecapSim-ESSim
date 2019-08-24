package Classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import Classes.ReaderUtils.typeData;
import Classes.ReaderUtils.loadMode;

@SuppressWarnings("unchecked")
public class MonitoringReader {

	private int nbNodes;
	private List<List<Object>> data;

	public MonitoringReader(int nbNodes, int vm, typeData type) {
		if (nbNodes != 3 && nbNodes != 9)
			throw new IllegalArgumentException("nbNodes can only be 3 or 9 on MonitoringReader creation");

		this.nbNodes = nbNodes;

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
		String filepath = System.getProperty("user.dir") + File.separator + path + File.separator + fileName;

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
				double addRelTime = Double.parseDouble(ReaderUtils.getWord(line, 0, ","));
				Date addAbsTime = ReaderUtils.readTimeMonitoring(ReaderUtils.getWord(line, line.indexOf(",") + 1, ","));
				double addValue = Double
						.parseDouble(ReaderUtils.getWord(line, line.indexOf(",", line.indexOf(",") + 1) + 1, ","));

				columns.get(0).add(addRelTime);
				columns.get(1).add(addAbsTime);
				columns.get(2).add(addValue);
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		this.data = columns;
	}

	public List<List<Object>> getData() {
		return this.data;
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
	public List<List<Object>> cleanDataset(WorkloadReader wReader) {

		List<List<Object>> validRequest = wReader.getData();
		long startTime = (Long) validRequest.get(0).get(0);
		long endTime = (Long) validRequest.get(0).get(validRequest.get(0).size() - 1);

		// clean all values out of specified bounds
		int time = 0;
		List<List<Object>> dataset = new ArrayList<>();
		Collections.copy(dataset, data);

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
	 * returns the fitDistribution for the given parameters and the specified
	 * loadMode
	 * 
	 * @param nbDNs
	 * @param compId
	 * @param type
	 * @param precision
	 * @param load
	 * @return
	 */
	public double[] getParamsDist(String componentId, double precision, loadMode load) {
		int compId = Integer.valueOf(componentId);

		if (compId < 3 || compId > 2 + nbNodes)
			throw new IllegalArgumentException(
					"componentId must point to one of the datanodes (3 <= componentId <= 2+nbDNs)");

		// TODO check typeData to avoid having less than two modes

		List<Double> dataset = (List<Double>) (List<?>) data.get(2);

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
