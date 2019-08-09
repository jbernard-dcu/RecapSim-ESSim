package Distribution;

import org.apache.commons.math3.analysis.ParametricUnivariateFunction;
import org.apache.commons.math3.analysis.integration.BaseAbstractUnivariateIntegrator;
import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.util.FastMath;

/***
 * Class implementing the ParametricUnivariateFunction interface, modeling the
 * repartition function of the gamma distribution
 * 
 * @author Joseph
 *
 */
public class GammaFunc implements ParametricUnivariateFunction {

	private double avg;
	private BaseAbstractUnivariateIntegrator itg;

	static final int maxEvals = Integer.MAX_VALUE;

	public GammaFunc(double avg, BaseAbstractUnivariateIntegrator itg) {
		super();
		this.avg = avg;
		this.itg = itg;
	}

	public double value(double x, double... parameters) {
		double alpha = parameters[0];
		double beta = alpha / this.avg;

		double intValue = itg.integrate(maxEvals, t -> Math.pow(t, alpha - 1) * Math.exp(-t / beta), 0, x);

		return (1. / (Gamma.gamma(alpha) * Math.pow(beta, alpha))) * intValue;
	}

	public double[] gradient(double x, double... parameters) {
		double alpha = parameters[0];
		double beta = alpha / this.avg;

		// We set F(x,alpha,beta)=C(alpha,beta)*I(x,alpha,beta)
		// Calculation of partial derivates
		double C = 1 / (Gamma.gamma(alpha) * Math.pow(beta, alpha));

		double I = itg.integrate(maxEvals, t -> Math.pow(t, alpha - 1) * Math.exp(-t / beta), 0, x);

		double dGammadalpha = itg.integrate(maxEvals, t -> Math.log(t) * Math.pow(t, alpha - 1), 0, Double.MAX_VALUE);
		double dCdalpha = (dGammadalpha + Gamma.gamma(alpha) * FastMath.log(beta))
				/ (Math.pow(beta, alpha) * Math.pow(Gamma.gamma(alpha), 2));

		double dIdalpha = itg.integrate(maxEvals, t -> Math.log(t) * Math.pow(t, alpha - 1) * Math.exp(-t / beta), 0,
				x);

		double dCdbeta = (-alpha / Math.pow(beta, alpha + 1) * Gamma.gamma(alpha)) * C;

		double dIdbeta = Math.pow(beta, -2)
				* itg.integrate(maxEvals, t -> Math.pow(t, alpha) * Math.exp(-t / beta), 0, x);

		return new double[] { dCdalpha * I + C * dIdalpha, dCdbeta * I + C * dIdbeta };

	}
}
