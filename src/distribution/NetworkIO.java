package distribution;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.util.Pair;

import utils.MonitoringReader;
import utils.TxtUtils;
import utils.TxtUtils.loadMode;
import utils.TxtUtils.typeData;

public class NetworkIO {

	private int nbNodes;
	private int[] vmIds;
	private Map<Pair<String, loadMode>, NormalDistribution> distReceived;
	private Map<Pair<String, loadMode>, NormalDistribution> distSent;

	private NetworkIO(int nbNodes) {
		this.nbNodes = nbNodes;
		this.vmIds = TxtUtils.getMonitoringVmsList(nbNodes);
		this.distReceived = new HashMap<Pair<String, loadMode>, NormalDistribution>();
		this.distSent = new HashMap<Pair<String, loadMode>, NormalDistribution>();
	}

	public static NetworkIO create(int nbNodes) {
		return new NetworkIO(nbNodes);
	}

	private void setDistReceived(Map<Pair<String, loadMode>, NormalDistribution> newDist) {
		this.distReceived = newDist;
	}

	private void setDistSent(Map<Pair<String, loadMode>, NormalDistribution> newDist) {
		this.distSent = newDist;
	}

	public double sampleReceivedMB(String componentId, loadMode mode) {
		return sampleIOMB(componentId, mode, distReceived, typeData.NetworkReceived);
	}

	public double sampleSentMB(String componentId, loadMode mode) {
		return sampleIOMB(componentId, mode, distSent, typeData.NetworkSent);
	}

	private double sampleIOMB(String componentId, loadMode mode, Map<Pair<String, loadMode>, NormalDistribution> map,
			typeData type) {

		Pair<String, loadMode> key = new Pair<String, loadMode>(componentId, mode);

		if (!map.containsKey(key)) {
			int compId = Integer.valueOf(componentId) - 3;
			MonitoringReader mReader = MonitoringReader.create(nbNodes, vmIds[compId], type).filter(mode);

			double[] params = mReader.getParamsDist(0.25);

			map.put(key, new NormalDistribution(params[0], params[1]));
		}

		switch (type) {
		case NetworkReceived:
			setDistReceived(map);
			break;
		case NetworkSent:
			setDistSent(map);
			break;
		default:
			break;
		}

		return map.get(key).sample();

	}

}
