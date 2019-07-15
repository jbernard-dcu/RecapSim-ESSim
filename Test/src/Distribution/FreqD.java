package Distribution;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class FreqD<T> {

	private List<T> dataset;
	private List<Double> freq;
	private List<Double> cumSum;

	public FreqD(List<T> dataset, List<Double> freq) {
		this.dataset = dataset;
		this.freq = freq;
		this.cumSum = calculateCumSum(this.freq);
	}

	public FreqD(TreeMap<T, Double> termDist) {
		this.dataset = new ArrayList<T>();
		this.freq = new ArrayList<Double>();

		this.dataset.addAll(termDist.keySet());
		this.freq.addAll(termDist.values());

		this.cumSum = calculateCumSum(this.freq);
	}

	public List<Double> calculateCumSum(List<Double> freq) {
		List<Double> cumSum = new ArrayList<Double>();
		cumSum.add(this.freq.get(0));
		for (int i = 1; i < this.dataset.size(); i++) {
			cumSum.add(cumSum.get(i - 1) + this.freq.get(i));
		}
		return cumSum;
	}

	public T sample() {		
		Double r = Math.random() * cumSum.get(cumSum.size() - 1);
		int i = 0;
		while (r > cumSum.get(i)) {
			i++;
		}
		return this.dataset.get(i);
	}

}
