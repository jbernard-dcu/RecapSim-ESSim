package distribution;

import java.util.ArrayList;
import java.util.List;

import utils.MonitoringReader;
import utils.TxtUtils;
import utils.TxtUtils.typeData;

@SuppressWarnings("unchecked")
public class MemoryUsage {

	private int nbNodes;
	private int vm;
	private typeData type;

	private MemoryUsage(int nbNodes, String componentId, typeData type) {
		this.nbNodes = nbNodes;
		this.vm = TxtUtils.getMonitoringVmsList(nbNodes)[Integer.valueOf(componentId) - 3];
		this.type = type;
	}

	public static MemoryUsage create(int nbNodes, String componentId, typeData type) {
		return new MemoryUsage(nbNodes, componentId, type);
	}

	private List<Double> getCumulativeOutput() {
		List<Double> output = (List<Double>) (List<?>) MonitoringReader.create(nbNodes, vm, type).getData().get(2);
		for (int i = 1; i < output.size(); i++) {
			output.set(i, output.get(i - 1) + output.get(i));
		}
		return output;
	}

	private List<Double> getTime() {
		return (List<Double>) (List<?>) MonitoringReader.create(nbNodes, vm, type).getData().get(0);
	}

	// eps, w0, wn
	private double[] getParameters() {
		List<Double> cumOutput = getCumulativeOutput();
		int i = 1;
		while (!(cumOutput.get(i - 1) < cumOutput.get(i) && cumOutput.get(i) == cumOutput.get(i + 1)))
			i++;
		double tpic = getTime().get(i);

		double eps = 0.7;
		double w0 = Math.PI / tpic;
		double wN = w0 / Math.sqrt(1 - Math.pow(eps, 2));

		return new double[] { eps, w0, wN };
	}
	
	public List<Double> getEmpiricOutput(){
		for(double t:getTime()) {
			
		}
	}

}
