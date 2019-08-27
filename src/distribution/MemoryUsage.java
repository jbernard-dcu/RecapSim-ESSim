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

	private List<Double> time;
	private List<Double> cInput;

	public static void main(String[] args) {
		int nbNodes = 9;
		int vm = 1;

		int[] vmIds = TxtUtils.getMonitoringVmsList(9);
		int vmId = vmIds[vm - 1];

		MemoryUsage mUsage = MemoryUsage.create(nbNodes, vmId);

		for(double i:mUsage.getEmpiricOutput()) {
			System.out.println(i);
		}
	}

	private MemoryUsage(int nbNodes, int vm) {
		this.nbNodes = nbNodes;
		this.vm = vm;

		List<List<Object>> data = MonitoringReader.create(nbNodes, vm, typeData.NetworkReceived).getData();
		this.time = (List<Double>) (List<?>) data.get(0);
		this.cInput = (List<Double>) (List<?>) data.get(2);
		for (int i = 1; i < cInput.size(); i++) {
			cInput.set(i, cInput.get(i - 1) + cInput.get(i));
		}
	}

	public static MemoryUsage create(int nbNodes, int vm) {
		return new MemoryUsage(nbNodes, vm);
	}

	// w0
	private double getParameter() {
		int i = 1;
		while (i < cInput.size() - 1) {
			if (cInput.get(i) > cInput.get(i - 1) && cInput.get(i) > cInput.get(i + 1))
				break;
			i++;
		}

		return Math.PI / time.get(i); // pi/tpic
	}

	public List<Double> getEmpiricOutput() {
		List<Double> empOutput = new ArrayList<>();
		double eps = 0.7;
		double eps2 = Math.sqrt(1 - Math.pow(eps, 2));
		double phi = Math.PI / 2.;
		double w0 = getParameter();
		double wN = w0 / eps2;

		for (int i = 0; i < time.size(); i++) {
			double 
			empOutput.add(cInput.get(i)
					* (1 - (Math.exp(-eps * wN * time.get(i) / eps2) / eps2) * Math.sin(w0 * time.get(i) + phi)));
		}

		return empOutput;
	}

}
