package distribution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.math3.distribution.NormalDistribution;

import utils.MonitoringReader;
import utils.TxtUtils;
import utils.TxtUtils.loadMode;
import utils.TxtUtils.typeData;

/***
 * The class that holds the NetworkIO distribution extracted from monitoring
 * files. The distributions are stored in Maps, associating each
 * NormalDistribution with a String containing componentId+loadMode
 * 
 * @author Joseph
 *
 */
public class NetworkIO {

	private int nbNodes;
	private int[] vmIds;
	private Map<String, NormalDistribution> distReceived;
	private Map<String, NormalDistribution> distSent;

	private NetworkIO(int nbNodes) {
		this.nbNodes = nbNodes;
		this.vmIds = TxtUtils.getMonitoringVmsList(nbNodes);
		this.distReceived = new HashMap<String, NormalDistribution>();
		this.distSent = new HashMap<String, NormalDistribution>();
	}

	public static NetworkIO create(int nbNodes) {
		return new NetworkIO(nbNodes);
	}

	private void setDistReceived(Map<String, NormalDistribution> map) {
		this.distReceived = map;
	}

	private void setDistSent(Map<String, NormalDistribution> newDist) {
		this.distSent = newDist;
	}

	public double sampleReceivedMB(String componentId, loadMode mode) {
		return sampleIOMB(componentId, mode, distReceived, typeData.NetworkReceived);
	}

	public double sampleSentMB(String componentId, loadMode mode) {
		return sampleIOMB(componentId, mode, distSent, typeData.NetworkSent);
	}

	private double sampleIOMB(String componentId, loadMode mode, Map<String, NormalDistribution> map, typeData type) {

		String key = componentId + mode;

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

	public double sumSampleSent(loadMode mode) {
		double sum = 0;

		List<String> compIds = Stream.iterate(3, n -> n + 1).limit(nbNodes).map(s -> "" + s)
				.collect(Collectors.toList());

		for (String compId : compIds) {
			sum += sampleSentMB(compId, mode);
		}
		return sum;
	}
}
