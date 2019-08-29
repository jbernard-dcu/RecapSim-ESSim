package distribution;

import utils.MonitoringReader;
import utils.TxtUtils;
import utils.TxtUtils.loadMode;
import utils.TxtUtils.typeData;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.NormalDistr;

import java.util.HashMap;
import java.util.Map;

public class Usage {

	private int nbNodes;
	private int[] vmIds;
	private Map<String, ContinuousDistribution> dist;
	private typeData type;

	private Usage(int nbNodes, typeData type) {
		this.nbNodes = nbNodes;
		this.vmIds = TxtUtils.getMonitoringVmsList(nbNodes);
		this.dist = new HashMap<String, ContinuousDistribution>();
		this.type = type;
	}

	public static Usage create(int nbNodes, typeData type) {
		return new Usage(nbNodes, type);
	}

	public double sampleUsage(String componentId, loadMode mode) {

		String key = componentId + mode;

		if (!dist.containsKey(key)) {
			int compId = Integer.valueOf(componentId) - 3;
			MonitoringReader mReader = MonitoringReader.create(nbNodes, vmIds[compId], type).filter(mode);

			double precision = Math.log(mReader.getNbPoints()) / 2 * Math.log(2);

			double mult = (type == typeData.CpuLoad) ? 0.01 : 1;
			switch (type) {
			case NetworkReceived:
			case NetworkSent:
			case CpuLoad:
				double[] params = mReader.getParamsDist(precision);
				dist.put(key, new NormalDistr(mult * params[0], mult * params[1]));
				break;
			default:
				break;
			}

		}

		return dist.get(key).sample();

	}

	public ContinuousDistribution getDistribution(String componentId, loadMode mode) {
		return dist.get(componentId + mode);
	}

}
