package txtutils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import txtutils.TxtUtils.loadMode;
import txtutils.TxtUtils.typeData;

@SuppressWarnings("unchecked")
public class MonitoringReader {

	private int nbNodes;
	private List<List<Object>> data;

	/**
	 * Constructor to build the data from specified arguments
	 * 
	 * @param nbNodes
	 * @param vm
	 * @param type
	 */
	private MonitoringReader(int nbNodes, int vm, typeData type) {
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
				double addRelTime = Double.parseDouble(TxtUtils.getWord(line, 0, ","));
				Date addAbsTime = TxtUtils.readTimeMonitoring(TxtUtils.getWord(line, line.indexOf(",") + 1, ","));
				double addValue = Double
						.parseDouble(TxtUtils.getWord(line, line.indexOf(",", line.indexOf(",") + 1) + 1, ","));

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

	public static MonitoringReader create(int nbNodes, int vm, typeData type) {
		return new MonitoringReader(nbNodes, vm, type);
	}

	public List<List<Object>> getData() {
		return this.data;
	}

	public int getNbPoints() {
		return data.get(0).size();
	}

	/**
	 * Filters the values out of bound of the workload associated with this load
	 * mode. To separate the two modes, this method must be called
	 */
	public MonitoringReader filter(loadMode mode) {

		WorkloadReader wReader = WorkloadReader.create(nbNodes, mode);

		List<List<Object>> validRequest = wReader.getData();
		long startTime = (Long) validRequest.get(0).get(0);
		long endTime = (Long) validRequest.get(0).get(validRequest.get(0).size() - 1);

		// clean all values out of specified bounds
		int time = 0;
		while (time < data.get(1).size()) {
			long date = ((Date) data.get(1).get(time)).getTime();
			if (date <= startTime || date > endTime) {
				for (int field = 0; field < data.size(); field++) {
					data.get(field).remove(time);
				}
			} else {
				time++;
			}
		}

		return this;
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
	public double[] getParamsDist(double precision) {

		// TODO check typeData to avoid having less than two modes

		List<Double> dataset = (List<Double>) (List<?>) data.get(2);

		// Calculating specified freqDist
		TreeMap<Double, Integer> freqDist = getFrequencyDist(dataset, precision);

		return estimateGaussianDist(freqDist);
	}

	private double[] estimateGaussianDist(TreeMap<Double, Integer> freqDist) {
		double avg = 0;
		int count = 0;
		for (double value : freqDist.keySet()) {
			avg += value * freqDist.get(value);
			count += freqDist.get(value);
		}
		avg /= count;

		double std = 0;
		for (double value : freqDist.keySet()) {
			std += Math.pow(value - avg, 2) * freqDist.get(value);
		}
		std = Math.sqrt(std / count);

		return new double[] { avg, std };
	}

	/**
	 * Returns the distribution by classes of the specified data, the precision must
	 * be
	 * set
	 * 
	 * @param nbNodes
	 * @param type
	 * @param vm
	 */
	private TreeMap<Double, Integer> getFrequencyDist(List<Double> dataset, double precision) {

		TreeMap<Double, Integer> res = new TreeMap<Double, Integer>();
		final int nbClasses = 1 + (int) (Collections.max(dataset) / precision);

		for (int i = 0; i <= nbClasses; i++) {
			res.put(precision * i, 0);
		}

		for (double value : dataset) {
			double key = precision * (int) (value / precision);
			res.put(key, res.get(key) + 1);
		}

		printHistogram(res, 0);

		return res;
	}

	private void printHistogram(TreeMap<Double, Integer> res, long waitingTimeMillis) {
		for (double key : res.keySet()) {
			String s = "";
			for (int i = 0; i < res.get(key).intValue(); i++) {
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

}
