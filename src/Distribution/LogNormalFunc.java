package Distribution;

import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.special.Erf;

/***
 * The class modeling the CDF of a log-normally distributed variable with a
 * known average. This class implements the {@link ParametricUnivariateFunction}
 * interface
 * 
 * @author Joseph
 *
 */
public class LogNormalFunc implements ParametricUnivariateFunction {

	// the mean of the log normally distributed variable
	private double avg;

	public LogNormalFunc(double avg) {
		this.avg = avg;
	}

	public double value(double x, double... parameters) {
		// we consider that mu is already known for this distribution
		double sigma = parameters[0];
		double mu = Math.log(this.avg) - Math.pow(sigma, 2) / 2;

		return .5 * (1 + Erf.erf((Math.log(x) - mu) / (sigma * Math.sqrt(2))));
	}

	public double[] gradient(double x, double... parameters) {
		/*
		 * F(x,sigma) = (1/2)(1+erf((ln(x)-mu)/(sigma*sqrt(2)))) =
		 * (1/2)(1+erf(U(x,mu,sigma)))
		 * 
		 * dFdsigma = (1/2)(dUdsigma)(dErf(sigma)dsigma (U(x,mu,sigma)))
		 * 
		 * We are only calculating the derivate according to the parameter sigma, the
		 * mean is already fixed by our data
		 */

		double sigma = parameters[0];

		double dUdsigma = 1. / 2. * Math.sqrt(2) - (Math.log(x) - Math.log(avg)) / (Math.sqrt(2.) * Math.pow(sigma, 2));

		double U = (Math.log(x) - Math.log(avg)) / (sigma * Math.sqrt(2.)) + sigma / (2. * Math.sqrt(2.));
		double dErfdsigma = (2. / Math.sqrt(Math.PI)) * Math.exp(-Math.pow(U, 2));

		return new double[] { .5 * dUdsigma * dErfdsigma };

	}

}
