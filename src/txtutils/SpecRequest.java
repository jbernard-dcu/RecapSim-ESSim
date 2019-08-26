package txtutils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.fitting.SimpleCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

import Distribution.*;

/***
 * Class modeling the specifications of a request, as given in the workload
 * data.
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

	public static void main(String[] args) {

		String init = "INSERT: Count=3938, Max=595967, Min=4680, Avg=38746.84, 90=71679, 99=241791, 99.9=544767, 99.99=595967";
		SpecRequest specs = new SpecRequest(init);

		double startValue = 1.;

		LogNormalFunc f = new LogNormalFunc(specs.getAvgLatency());

		System.out.println("fit:" + specs.fitParameter(f, startValue));
		System.out.println("estimate:" + specs.estimateParameter(f, specs.getPercentile(0.9), 1E-17));

	}

	/**
	 * The <code>String</code> init should be of the following format for this
	 * constructor : </br>
	 * <code>"INSERT: Count=3938, Max=595967, Min=4680, Avg=38746.84, 90=71679, 99=241791, 99.9=544767, 99.99=595967"</code></br>
	 * The fields are automatically recognized and filled from this String
	 */
	public SpecRequest(String init) {
		this.type = TxtUtils.getWord(init, 0, ": ");
		this.countOp = Integer.parseInt(TxtUtils.getWord(init, init.indexOf("Count=") + 6, ", "));
		this.max = Integer.parseInt(TxtUtils.getWord(init, init.indexOf("Max=") + 4, ", "));
		this.min = Integer.parseInt(TxtUtils.getWord(init, init.indexOf("Min=") + 4, ", "));
		this.avg = Double.parseDouble(TxtUtils.getWord(init, init.indexOf("Avg=") + 4, ", "));
		this.q90 = Integer.parseInt(TxtUtils.getWord(init, init.indexOf("90=") + 3, ", "));
		this.q99 = Integer.parseInt(TxtUtils.getWord(init, init.indexOf("99=") + 3, ", "));
		this.q999 = Integer.parseInt(TxtUtils.getWord(init, init.indexOf("99.9=") + 5, ", "));
		this.q9999 = Integer.parseInt(init.substring(init.indexOf("99.99=") + 6));
	}

	/**
	 * Estimates the parameter based on one of the percentiles
	 */
	public double estimateParameter(ParametricUnivariateFunction f, double[] point, double precision) {

		double proba = point[0];
		double percentile = point[1];

		double param = 0;

		// We suppose from previous fits that the parameter should be between 1 and 2
		double[] x = new double[] { 1, 2 };
		double[] fx = new double[2];

		while (Math.abs(f.value(percentile, param) - proba) >= precision) {

			fx[0] = f.value(percentile, x[0]) - proba;
			fx[1] = f.value(percentile, x[1]) - proba;

			param = (fx[1] * x[0] - fx[0] * x[1]) / (fx[1] - fx[0]);

			x[0] = x[1];
			x[1] = param;
		}

		return param;

	}

	/**
	 * Returns a fitted alpha parameter for specified CDF in parameter. We assume
	 * that we're losing one degree of freedom since we already know the mean of our
	 * distribution
	 */
	public double fitParameter(ParametricUnivariateFunction f, double startValue) {
		WeightedObservedPoints dataObs = new WeightedObservedPoints();
		dataObs.add(this.q90, 0.9);
		dataObs.add(this.q99, 0.99);
		dataObs.add(this.q999, 0.999);
		dataObs.add(this.q9999, 0.9999);

		double[] parameters = new double[] { startValue };

		SimpleCurveFitter fitter = SimpleCurveFitter.create(f, parameters);
		double[] calculatedParam = fitter.fit(dataObs.toList());

		return calculatedParam[0];
	}

	public String toString() {
		List<Object> list = new ArrayList<>();
		list.addAll(Arrays.asList(type, countOp, max, min, avg, q90, q99, q999, q9999));
		return list.toString();
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

	public double[] getPercentile(double proba) {

		double percentile = (proba == 0.9) ? q90
				: (proba == 0.99) ? q99 : (proba == 0.999) ? q999 : (proba == 0.9999) ? q9999 : 0;
		if (percentile == 0)
			throw new IllegalArgumentException("proba must be 0.9, 0.99, 0.999 or 0.9999");

		return new double[] { proba, percentile };
	}
}