package Classes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.analysis.solvers.SecantSolver;
import org.apache.commons.math3.fitting.SimpleCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

import Distribution.*;

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

	public static void main(String[] args) {

		String init = "INSERT: Count=3938, Max=595967, Min=4680, Avg=38746.84, 90=71679, 99=241791, 99.9=544767, 99.99=595967";
		SpecRequest specs = new SpecRequest(init);

		double startValue = 5;

		// GammaFunc f = new GammaFunc(specs.getAvgLatency());
		LogNormalFunc f = new LogNormalFunc(specs.getAvgLatency());

		System.out.println("fit:" + specs.fitParameter(f, startValue));
		System.out.println("estimate:" + specs.estimateParameter(f, new double[] { 0.999, specs.q999 }, 1E-4));

	}

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
	 * Estimates the parameter based on one of the percentiles
	 */
	public double estimateParameter(ParametricUnivariateFunction f, double[] percentile, double precision) {

		double proba = percentile[0];
		double value = percentile[1];

		double[] param = { 0 };

		while (Math.abs(f.value(value, param) - proba) >= precision) {
			System.out.println("value="+f.value(value, param));
			System.out.println("param="+param[0]+"\n");
			param[0] += precision;
		}
		
		return Math.round(param[0]*Math.pow(10, -Math.log10(precision)))*precision;
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

		SimpleCurveFitter fitter = SimpleCurveFitter.create(f, parameters).withMaxIterations(100);
		double[] calculatedParam = fitter.fit(dataObs.toList());

		return calculatedParam[0];
	}

	public String toString() {
		List<Object> list = new ArrayList<>();
		list.addAll(Arrays.asList(type, countOp, max, min, avg, q90, q99, q999, q9999));
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

	public double[] getPercentile(int proba) {
		double percentile = 0;
		switch (proba) {
		case 90:
			percentile = q90;
			break;
		case 99:
			percentile = q99;
			break;
		case 999:
			percentile = q999;
			break;
		case 9999:
			percentile = q9999;
			break;
		default:
			throw new IllegalArgumentException("proba must be 90, 99, 999 or 9999");
		}

		return new double[] { proba, percentile };
	}
}