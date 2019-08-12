package Classes;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.fitting.SimpleCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

/***
 * Class modeling the specifications of a request, as given in the workload data
 * 
 * @author Joseph
 */
public class SpecRequest {
	private String type;
	private int countOp;
	private int max;
	private int min;
	private double avg;
	private double q90;
	private double q99;
	private double q999;
	private double q9999;

	public SpecRequest(String init) {
		this.type = TxtReader.getWord(init, 0, ": ");
		this.countOp = Integer.parseInt(TxtReader.getWord(init, init.indexOf("Count=") + 6, ", "));
		this.max = Integer.parseInt(TxtReader.getWord(init, init.indexOf("Max=") + 4, ", "));
		this.min = Integer.parseInt(TxtReader.getWord(init, init.indexOf("Min=") + 4, ", "));
		this.avg = Double.parseDouble(TxtReader.getWord(init, init.indexOf("Avg=") + 4, ", "));
		this.q90 = Integer.parseInt(TxtReader.getWord(init, init.indexOf("90=") + 3, ", "));
		this.q99 = Integer.parseInt(TxtReader.getWord(init, init.indexOf("99=") + 3, ", "));
		this.q999 = Integer.parseInt(TxtReader.getWord(init, init.indexOf("99.9=") + 5, ", "));
		this.q9999 = Integer.parseInt(init.substring(init.indexOf("99.99=") + 6));
	}

	/**
	 * Returns a fitted alpha parameter for the gamma distribution fit. We assume
	 * that the parameter beta is linearly correlated to alpha since we already know
	 * the mean of our distribution
	 */
	public double fitParameter(ParametricUnivariateFunction f, double startValue) {
		WeightedObservedPoints dataObs = new WeightedObservedPoints();

		double div = 1E3;

		dataObs.add(this.q90 / div, 0.9);
		dataObs.add(this.q99 / div, 0.99);
		dataObs.add(this.q999 / div, 0.999);
		dataObs.add(this.q9999 / div, 0.9999);

		double[] parameters = new double[] { startValue };

		SimpleCurveFitter fitter = SimpleCurveFitter.create(f, parameters).withMaxIterations(100);
		double[] calculatedParam = fitter.fit(dataObs.toList());

		return calculatedParam[0];
	}

	public String toString() {
		List<Object> list = new ArrayList<Object>();
		list.add(type);
		list.add(countOp);
		list.add(max);
		list.add(min);
		list.add(avg);
		list.add(q90);
		list.add(q99);
		list.add(q999);
		list.add(q9999);
		return list.toString() + "\n";
	}

	public String getType() {
		return this.type;
	}

	public double getAvgLatency() {
		return this.avg;
	}

	public int getOpCount() {
		return this.countOp;
	}
}