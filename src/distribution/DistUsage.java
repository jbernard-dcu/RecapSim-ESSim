package distribution;

import utils.MonitoringReader;
import utils.TxtUtils;
import utils.TxtUtils.loadMode;
import utils.TxtUtils.typeData;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;

import java.util.HashMap;
import java.util.Map;

public class DistUsage {

	private int nbNodes;
	private int[] vmIds;
	private Map<String, RealDistribution> dist;
	private typeData type;

	private DistUsage(int nbNodes, typeData type) {
		this.nbNodes = nbNodes;
		this.vmIds = TxtUtils.getMonitoringVmsList(nbNodes);
		this.dist = new HashMap<String, RealDistribution>();
		this.type = type;
	}

	public static DistUsage create(int nbNodes, typeData type) {
		return new DistUsage(nbNodes, type);
	}

	public double sampleUsage(String componentId, loadMode mode) {

		String key = componentId + mode;

		if (!dist.containsKey(key)) {
			int compId = Integer.valueOf(componentId) - 3;
			MonitoringReader mReader = MonitoringReader.create(nbNodes, vmIds[compId], type).filter(mode);

			double precision = Math.log(mReader.getNbPoints()) / 2 * Math.log(2);
			switch (type) {
			case NetworkReceived:
			case NetworkSent:
			case CpuLoad:
				double[] params = mReader.getParamsDist(precision);
				dist.put(key, new NormalDistribution(params[0], params[1]));
				break;
			default:
				break;
			}
			
		}

		return dist.get(key).sample();

	}

}
