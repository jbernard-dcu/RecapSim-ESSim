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

	final static double relativeAccuracy = 1E-6;
	final static double absoluteAccuracy = 1E-3;
	final static int minimalIterationCount = 10;
	final static int maximalIterationCount = 64;

	static final int maxEvals = 100;

	public GammaFunc(double avg) {
		super();
		this.avg = avg;
	}

	public double value(double x, double... parameters) {
		double alpha = parameters[0];
		double beta = alpha / this.avg;// <--
		// double beta = this.avg/alpha;
		// double beta = parameters[1];

		// TrapezoidIntegrator itg = new TrapezoidIntegrator();
		// double intValue = itg.integrate(maxEvals, t -> Math.pow(t, alpha - 1) *
		// Math.exp(-t / beta), 0, x);
		double intValue = integrate(t -> Math.pow(t, alpha - 1) * Math.exp(-t / beta), 0, x, 1);

		return (1. / (Gamma.gamma(alpha) * Math.pow(beta, alpha))) * intValue;
	}

	public double[] gradient(double x, double... parameters) {
		double alpha = parameters[0];
		double beta = alpha / this.avg;// <--
		// double beta = this.avg/alpha;
		// double beta = parameters[1];

		double zero = 1E-25;

		// We have F(x,alpha,beta)=C(alpha,beta)*I(x,alpha,beta)
		// Calculation of partial derivates
		double C = 1 / (Gamma.gamma(alpha) * Math.pow(beta, alpha));

		// TrapezoidIntegrator it1 =new TrapezoidIntegrator(relativeAccuracy,
		// absoluteAccuracy,
		// minimalIterationCount, maximalIterationCount);
		// double I = it1.integrate(maxEvals, t -> Math.pow(t, alpha - 1) * Math.exp(-t
		// / beta), 0, x);
		double I = integrate(t -> Math.pow(t, alpha - 1) * Math.exp(-t / beta), 0, x, 2);

		double dGammadalpha = Gamma.gamma(alpha) * Gamma.digamma(alpha);
		double dCdalpha = (dGammadalpha + Gamma.gamma(alpha) * Math.log(beta))
				/ (Math.pow(beta, alpha) * Math.pow(Gamma.gamma(alpha), 2));

		// TrapezoidIntegrator it2 = new TrapezoidIntegrator(relativeAccuracy,
		// absoluteAccuracy,
		// minimalIterationCount, maximalIterationCount);
		// double dIdalpha = it2.integrate(maxEvals, t -> Math.log(t) * Math.pow(t,
		// alpha - 1) * Math.exp(-t / beta), 0,
		// x);
		double dIdalpha = integrate(t -> Math.log(t) * Math.pow(t, alpha - 1) * Math.exp(-t / beta), zero, x, 3);

		double dCdbeta = (-alpha / Math.pow(beta, alpha + 1) * Gamma.gamma(alpha)) * C;

		// TrapezoidIntegrator it3 = new TrapezoidIntegrator(relativeAccuracy,
		// absoluteAccuracy,
		// minimalIterationCount, maximalIterationCount);
		// double dIdbeta = Math.pow(beta, -2)
		// * it3.integrate(maxEvals, t -> Math.pow(t, alpha) * Math.exp(-t / beta), 0,
		// x);
		double dIdbeta = integrate(t -> Math.pow(t, alpha) * Math.exp(-t / beta), 0, x, 4);

		return new double[] { dCdalpha * I + C * dIdalpha, dCdbeta * I + C * dIdbeta };

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
