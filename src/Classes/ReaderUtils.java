package Classes;

import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class ReaderUtils {

	public static enum typeData {
		CpuLoad, DiskIOReads, DiskIOWrites, MemoryUsage, NetworkReceived, NetworkSent
	}

	public enum loadMode {
		WRITE, READ;
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

	/**
	 * Reads the time in the format specified in monitoring files and returns the
	 * value as a {@link Date}
	 */
	public static Date readTimeMonitoring(String time) {

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

}
