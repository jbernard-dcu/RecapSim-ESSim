package Classes;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import Classes.ReaderUtils.typeData;

public class TxtReader {

	@SuppressWarnings("unchecked")
	public static List<Double> getWaitingTimesReads(int nbNodes, int vm) {

		List<List<Object>> monitoringData = new MonitoringReader(nbNodes, vm, typeData.DiskIOReads).getData();

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

	@SuppressWarnings("unchecked")
	public static List<Double> getValuesNonZeroReads(int nbNodes, int vm) {

		List<Double> values = (List<Double>) (List<?>) new MonitoringReader(nbNodes, vm, typeData.DiskIOReads).getData()
				.get(2);
		return values.stream().filter(value -> value > 0).collect(Collectors.toList());
	}

	/**
	 * Method to calculate the approx. repartition of the requests among the data
	 * nodes, based on specified type of data
	 */
	public static double[] calculateRepartNodes(int nbNodes, typeData type) {

		int[] vms = ReaderUtils.getMonitoringVmsList(nbNodes);

		// getting cpuLoads
		List<List<Double>> values = new ArrayList<>();
		for (int vm : vms) {
			@SuppressWarnings("unchecked")
			List<Double> add = (List<Double>) (List<?>) new MonitoringReader(nbNodes, vm, type).getData().get(2);

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

}