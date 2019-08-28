package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import utils.TxtUtils.typeData;

public class TransferFunction {

	public static void main(String[] args) {
		int nbNodes = 9;
		int vm = 3; // between [1:nbNodes]

		int[] vms = TxtUtils.getMonitoringVmsList(nbNodes);
		int vmId = vms[vm - 1];

		List<Double> input = getDataset(nbNodes, vmId, typeData.NetworkReceived);
		List<Double> output = getDataset(nbNodes, vmId, typeData.MemoryUsage);

		MonitoringReader mrInput = MonitoringReader.create(nbNodes, vmId, typeData.NetworkReceived);
		MonitoringReader mrOutput = MonitoringReader.create(nbNodes, vmId, typeData.MemoryUsage);

		List<Double> transfer = getTransfer(mrInput, mrOutput);
		transfer = transfer.stream().map(s->s*1_000).collect(Collectors.toList());
		
		List<Long> time = getTime(nbNodes, vmId, typeData.NetworkReceived);

		// double precision = 0.01;
		// Map<Double,Integer> freqDist = MonitoringReader.getFrequencyDist(transfer,
		// precision);

		for (int i = 0; i < time.size(); i++) {
			System.out.println(i + " " + transfer.get(i));
		}
		
		TreeMap<Double,Integer> freqDist = MonitoringReader.getFrequencyDist(transfer, 0.01);
		MonitoringReader.printHistogram(freqDist, 10);

		/*
		 * List<Double> diskReads = TxtUtils.getValuesNonZeroReads(nbNodes, vmId);
		 * List<Double> wtReads = TxtUtils.getWaitingTimesReads(nbNodes, vmId);
		 * 
		 * TreeMap<Double, Integer> vnzDist =
		 * MonitoringReader.getFrequencyDist(diskReads, 50);
		 * MonitoringReader.printHistogram(vnzDist, 0);
		 * 
		 * TreeMap<Double, Integer> wtDist = MonitoringReader.getFrequencyDist(wtReads,
		 * 10);
		 * MonitoringReader.printHistogram(wtDist, 0);
		 * 
		 * List<Double> difOut = getDifOutput(nbNodes, vmId);
		 * for (double i : difOut) {
		 * System.out.println(i);
		 * }
		 */
	}

	public static List<Double> getDataset(int nbNodes, int vm, typeData type) {
		return (List<Double>) (List<?>) MonitoringReader.create(nbNodes, vm, type).getData().get(2);
	}

	public static List<Long> getTime(int nbNodes, int vm, typeData type) {
		return (List<Long>) (List<?>) MonitoringReader.create(nbNodes, vm, type).getData().get(0);
	}

	public static List<Double> getTransfer(MonitoringReader mInput, MonitoringReader mOutput) {
		List<List<Object>> input = mInput.getData();
		List<List<Object>> output = mOutput.getData();

		List<Long> timeIn = (List<Long>) (List<?>) input.get(1);
		List<Long> timeOut = (List<Long>) (List<?>) output.get(1);

		long startInput = timeIn.get(0);
		long startOutput = timeOut.get(0);
		long min = Math.max(startInput, startOutput);

		long endInput = timeIn.get(timeIn.size() - 1);
		long endOutput = timeOut.get(timeOut.size() - 1);
		long max = Math.min(endInput, endOutput);

		List<List<Object>> aligned = new ArrayList<>();
		for (int field = 0; field < input.size(); field++) {
			aligned.add(new ArrayList<>());
		}
		for (long time = min; time <= max; time += 10 * 1000) {
			int indexTimeIn = input.get(1).indexOf(time);
			int indexTimeOut = output.get(1).indexOf(time);
			aligned.get(0).add(0);
			aligned.get(1).add(time);
			double add = (double) output.get(2).get(indexTimeOut) / (double) input.get(2).get(indexTimeIn);
			aligned.get(2).add(add);
		}

		return (List<Double>) (List<?>) aligned.get(2);

	}

	public static List<Double> getTransfer(List<Double> inputDataset, List<Double> outputDataset) {
		List<Double> transfer = new ArrayList<>();
		for (int i = 0; i < inputDataset.size(); i++) {
			transfer.add(outputDataset.get(i) / inputDataset.get(i));
		}
		return transfer;
	}

	public static List<Double> getDifOutput(int nbNodes, int vm) {
		List<Double> res = (List<Double>) (List<?>) MonitoringReader.create(nbNodes, vm, typeData.MemoryUsage).getData()
				.get(2);
		List<Double> input = getDataset(nbNodes, vm, typeData.NetworkReceived);
		List<Long> time = getTime(nbNodes, vm, typeData.NetworkReceived);

		for (double t : time) {
			int indextres = res.indexOf(t);
			int indexinput = input.indexOf(t);
			if (indextres > 0)
				res.set(indextres, res.get(indextres) - input.get(indexinput));
		}

		double avg = res.parallelStream().mapToDouble(s -> s).average().getAsDouble();
		double init = res.get(0);

		for (int i = 0; i < res.size(); i++) {
			res.set(i, res.get(i) - init);
		}

		return res;
	}

}
