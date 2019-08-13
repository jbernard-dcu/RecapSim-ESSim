package Distribution;

import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.special.Gamma;

/***
 * Class implementing the ParametricUnivariateFunction interface, modeling the
 * repartition function of the gamma distribution
 * 
 * @author Joseph
 *
 */
public class GammaFunc implements ParametricUnivariateFunction {

	private double avg;

	public GammaFunc(double avg) {
		super();
		this.avg = avg;
	}

	public double value(double x, double... parameters) {
		double alpha = parameters[0];

		double intValue = integrate(t -> Math.pow(t, alpha - 1) * Math.exp(-t * alpha / avg), 0, x, 1);

		return intValue * Math.pow(alpha / avg, alpha) / Gamma.gamma(alpha);
	}

	public double[] gradient(double x, double... parameters) {
		double alpha = parameters[0];

		double zero = Double.MIN_VALUE;

		/*
		 * F(x,alpha) = C(alpha)*I(x,alpha)
		 */

		double C = Math.pow(alpha / avg, alpha) / Gamma.gamma(alpha);

		double I = integrate(t -> Math.pow(t, alpha - 1) * Math.exp(-alpha * t / avg), 0, x, 2);

		double dCdalpha = (Math.pow(alpha / avg, alpha) * (1 + Math.log(alpha / avg)) + Gamma.digamma(alpha))
				/ Gamma.gamma(alpha);

		double dIdalpha = integrate(t -> Math.pow(t, alpha - 1) * Math.exp(-alpha * t / avg) * (Math.log(t) - t / avg),
				zero, x, 3);

		return new double[] { dCdalpha * I + C * dIdalpha };

	}

	public static double integrate(UnivariateFunction f, double lowerBound, double upperBound, int i) {
		System.out.println("lb=" + lowerBound + " ub=" + upperBound);
		System.out.println("f(lb)=" + f.value(lowerBound) + " f(ub)=" + f.value(upperBound));
		System.out.println("f(m)=" + f.value((lowerBound + upperBound) / 2.));

		double integrale = ((upperBound - lowerBound) / 6.)
				* (f.value(lowerBound) + f.value(upperBound) + 4 * f.value((lowerBound + upperBound) / 2.));
		
		System.out.println("function:" + i + " value:" + integrale + "\n");
		return integrale;
	}
}
