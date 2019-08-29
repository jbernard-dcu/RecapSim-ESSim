package essim;

import java.util.HashMap;
import java.util.Map;

import org.cloudbus.cloudsim.distributions.NormalDistr;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelAbstract;

public class UtilizationModelNormal extends UtilizationModelAbstract {

	private Map<Double, Double> history;
	private NormalDistr randomGen;

	public UtilizationModelNormal(double[] params) {
		super();
		setHistory(new HashMap<>());
		setRandomGenerator(new NormalDistr(params[0], params[1]));
	}

	public UtilizationModelNormal(Unit unit, double[] params) {
		this(params);
		setUnit(unit);
	}

	private void setRandomGenerator(NormalDistr normalDistr) {
		this.randomGen = normalDistr;
	}

	private void setHistory(Map<Double, Double> history) {
		this.history = history;

	}

	public double getUtilization(double time) {
		if (getHistory().containsKey(time)) {
			return getHistory().get(time);
		}

		final double utilization = getRandomGenerator().sample();
		getHistory().put(time, utilization);
		return utilization;
	}

	private NormalDistr getRandomGenerator() {
		return randomGen;
	}

	private Map<Double, Double> getHistory() {
		return history;
	}

}
