package Distribution;

import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.special.Erf;

public class LogNormaleFunc implements ParametricUnivariateFunction {

	// the mean of the log normally distributed variable
	private double avg;

	public LogNormaleFunc(double avg) {
		this.avg = avg;
	}

	@Override
	public double value(double x, double... parameters) {
		// double mu = parameters[0];
		// we consider that mu is already known for this distribution
		double sigma = parameters[0];
		double mu = Math.log(this.avg) - Math.pow(sigma, 2) / 2;

		return .5 * (1 + Erf.erf((Math.log(x) - mu) / (sigma * Math.sqrt(2))));
	}

	@Override
	public double[] gradient(double x, double... parameters) {
		/*
		 * F(x,sigma) = (1/2)(1+erf((ln(x)-mu)/(sima*sqrt(2)))) =
		 * (1/2)(1+erf(U(x,mu,sigma)))
		 * 
		 * dFdsigma = (1/2)(dUdsigma)(dErf(sigma)dsigma (U(x,mu,sigma)))
		 * 
		 * We are only ccalculating the derivate according to the parameter sigma, the
		 * mean is already fixed by our data
		 */

		double sigma = parameters[0];

		double dUdsigma = 1. / 2. * Math.sqrt(2) - (Math.log(x) - Math.log(avg)) / (Math.sqrt(2.) * Math.pow(sigma, 2));

		double U = (Math.log(x) - Math.log(avg)) / (sigma * Math.sqrt(2.)) + sigma / (2. * Math.sqrt(2.));
		double dErfdsigma = (2. / Math.sqrt(Math.PI)) * Math.exp(-Math.pow(U, 2));

		return new double[] { .5 * dUdsigma * dErfdsigma };

	}

}
