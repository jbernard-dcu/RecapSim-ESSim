package Classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import Classes.ReaderUtils.typeData;

public class MonitoringReader {

	private int nbNodes;
	private typeData type;
	private int vm;

	private List<List<Object>> data;

	public MonitoringReader(int nbNodes, int vm, typeData type) {
		if (nbNodes != 3 && nbNodes != 9)
			throw new IllegalArgumentException("nbNodes can only be 3 or 9 on MonitoringReader creation");

		this.nbNodes = nbNodes;
		this.vm = vm;
		this.type = type;

		String filepath = getFilePath();

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

	public List<List<Object>> getData(){
		return this.data;
	}
	
	
	/**
	 * Builds and returns the filepath String of the monitoring file of nbNodes
	 * cluster and typeData values
	 * 
	 * @param nbNodes
	 * @param type
	 * @return
	 */
	public String getFilePath() {
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

}
