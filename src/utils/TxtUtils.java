package utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class TxtUtils {

	//////////////////////////////////////////////////////////////////////
	/////////////// TXTREADER
	//////////////////////////////////////////////////////////////////////

	public static List<Double> getWaitingTimesReads(int nbNodes, int vm) {

		List<List<Object>> monitoringData = MonitoringReader.create(nbNodes, vm, typeData.DiskIOReads).getData();

		List<Double> timestamps = (List<Double>) (List<?>) monitoringData.get(0);
		List<Double> values = (List<Double>) (List<?>) monitoringData.get(2);

		int previousOc = 0;

		List<Double> waitingTimes = new ArrayList<>();
		for (int time = 0; time < timestamps.size(); time++) {
			if (values.get(time) != 0) {
				waitingTimes.add(timestamps.get(time) - timestamps.get(previousOc));
				previousOc = time;
			}
		}

		return waitingTimes;
	}

	public static List<Double> getValuesNonZeroReads(int nbNodes, int vm) {

		List<Double> values = (List<Double>) (List<?>) MonitoringReader.create(nbNodes, vm, typeData.DiskIOReads)
				.getData().get(2);
		return values.stream().filter(value -> value > 0).collect(Collectors.toList());
	}

	/**
	 * Method to calculate the approx. repartition of the requests among the data
	 * nodes, based on specified type of data
	 */
	public static double[] calculateRepartNodes(int nbNodes, typeData type) {

		int[] vms = TxtUtils.getMonitoringVmsList(nbNodes);

		// getting cpuLoads
		List<List<Double>> values = new ArrayList<>();
		for (int vm : vms) {
			List<Double> add = (List<Double>) (List<?>) MonitoringReader.create(nbNodes, vm, typeData.DiskIOReads)
					.getData().get(2);

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

	//////////////////////////////////////////////////////////////////////
	/////////////// ENUMS
	//////////////////////////////////////////////////////////////////////

	public static enum typeData {
		CpuLoad, DiskIOReads, DiskIOWrites, MemoryUsage, NetworkReceived, NetworkSent
	}

	public enum loadMode {
		WRITE, READ;
	}

	//////////////////////////////////////////////////////////////////////
	/////////////// READTIME
	//////////////////////////////////////////////////////////////////////

	/**
	 * Reads the time in the format specified in monitoring files and returns the
	 * value as a {@link Date}
	 */
	public static long readTimeMonitoring(String time) {

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

		return new GregorianCalendar(year, month, day, hours, minutes, seconds).getTime().getTime();
	}

	public static long readTimeWorkload(String source) {
		int years = Integer.parseInt(getWord(source, 0, "-"));
		int months = Integer.parseInt(getWord(source, 5, "-"));
		int days = Integer.parseInt(getWord(source, 8, " "));
		int hours = Integer.parseInt(getWord(source, 11, ":"));
		int minutes = Integer.parseInt(getWord(source, 14, ":"));
		int seconds = Integer.parseInt(getWord(source, 17, ":"));
		int milliseconds = Integer.parseInt(getWord(source, 20, " "));
		return new GregorianCalendar(years, months, days, hours, minutes, seconds).getTimeInMillis() + milliseconds;
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

	public static List<List<Object>> mergeWorkloadsData(WorkloadReader w, WorkloadReader r) {
		List<List<Object>> requestsW;
		List<List<Object>> requestsR;

		if (w.getMode() == r.getMode()) {
			throw new IllegalArgumentException("Can't merge two workloads of the same mode");
		} else {
			if (w.getMode() == loadMode.READ) {
				requestsW = r.getData();
				requestsR = w.getData();
			} else {
				requestsR = r.getData();
				requestsW = w.getData();
			}

		}

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
	 * Returns an array containing the identifiers of VMs depending on the number of
	 * nodes considered. This method is useful to read monitoring files from the
	 * YCSB workload</br>
	 * TODO modify this method depending on the identifiers of the VMs
	 * 
	 * @param nbNodes
	 * @return
	 */
	public static int[] getMonitoringVmsList(int nbNodes) {
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

}
