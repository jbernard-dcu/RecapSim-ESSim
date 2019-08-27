package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import utils.TxtUtils.typeData;

public class TransferFunction {
	
	public static void main(String[] args) {
		int nbNodes = 9;
		int vm = 9; // between [1:nbNodes]
		
		int[] vms = TxtUtils.getMonitoringVmsList(nbNodes);
		int vmId = vms[vm-1];
		
		List<Double> input = getDataset(nbNodes, vmId, typeData.NetworkReceived);
		List<Double> output = getDataset(nbNodes,vmId,typeData.MemoryUsage);
		
		List<Double> transfer = getTransfer(input,output);
		
		double precision = 0.01;
		Map<Double,Integer> freqDist = MonitoringReader.getFrequencyDist(transfer, precision);
	}
	

	public static List<Double> getDataset(int nbNodes, int vm, typeData type) {
		return (List<Double>) (List<?>) MonitoringReader.create(nbNodes, vm, type).getData().get(2);
	}
	
	public static List<Double> getTransfer(List<Double> inputDataset, List<Double> outputDataset){
		List<Double> transfer = new ArrayList<>();
		for(int i=0;i<inputDataset.size();i++) {
			transfer.add(outputDataset.get(i)/inputDataset.get(i));
		}
		return transfer;
	}

}
