package Distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

public class FreqD<T> {

	private List<T> dataset;
	private List<Double> freq;
	private List<Double> cumSum;

	public FreqD(Collection<T> dataset, Collection<Double> freq) {
		if(dataset.size()!=freq.size())
			throw new IllegalArgumentException("dataset and freq must be the same size");
		
		this.dataset = new ArrayList<T>(dataset);
		this.freq = new ArrayList<Double>(freq);
		this.cumSum = calculateCumSum(this.freq);
	}

	public FreqD(TreeMap<T, Double> termDist) {
		this(termDist.keySet(), termDist.values());
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
