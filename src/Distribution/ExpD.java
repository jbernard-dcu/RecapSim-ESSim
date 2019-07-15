package Distribution;

public class ExpD {
	
	private double lambda;
	
	public ExpD(double lambda) {
		this.lambda=lambda;
	}
	
	public double sample() {
		return -Math.log(Math.random())/this.lambda;
	}

}
