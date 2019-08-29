package synthetic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.util.Pair;

import eu.recap.sim.models.WorkloadModel.Request;
import main.Launcher;

public class SynthUtils {
	
	/*
	 * Parameters synthetic workload
	 */
	// Parameters nbWord request
	public final static double AVG_NWREQ = 4.;
	public final static double STD_NWREQ = 2.;
	// Parameters nbWord document
	public final static int AVG_NWDOC = 10;
	public final static int STD_NWDOC = 5;
	// Parameters termSet and requestSet
	public final static int NB_TERMSET = 10_000;
	public final static int NB_REQUEST = 10;
	// Parameters database
	public final static int NB_DOCS = 1_000;

	/**
	 * Creates a formatted String giving the contents of a Request. </br>
	 * Change this method to change the format of the String.</br>
	 * Change proto file of WorkloadModel to change type of request content. </br>
	 */
	public static String randQueryContent(List<Pair<Long, Double>> termDist, int nbWord) {
		// Creating distribution
		EnumeratedDistribution<Long> dist = new EnumeratedDistribution<Long>(termDist);

		// Creating list of content
		List<Long> content = new ArrayList<Long>();
		long add;
		int word = 0;
		while (word < nbWord) {
			add = dist.sample();
			if (!content.contains(add)) {
				content.add(add);
				word++;
			}
		}

		// Parsing into a formatted String
		String rep = "";
		for (long w : content) {
			int len = (int) (Math.log10(w) + 1);
			rep = rep + len + w;
		}

		return rep;
	}

	/**
	 * Unparsing the formatted String for searchContent If changing the parsing
	 * method, change also this method
	 */
	public static List<Long> unparse(String searchContent) {
		List<Long> res = new ArrayList<Long>();
		int index = 0;
		int len;
		while (index < searchContent.length()) {
			len = Integer.valueOf(searchContent.charAt(index) - '0');
			res.add(Long.valueOf(searchContent.substring(index + 1, index + len + 1)));
			index += len + 1;
		}
		return res;
	}
	
	/**
	 * Returns the weight of the query</br>
	 * Change this method to change way of valorising requests.
	 */
	public static double getWeight(Request r, List<Pair<Long, Double>> termDist) {
		List<Long> content = unparse(r.getSearchContent());
		double score = 0.;
		for (long term : content) {
			score += termDist.get((int) term).getValue();
		}
		return score / content.size();
	}
	
	/**
	 * Creates a basic Zipfian distribution, useful to create the parameter of FreqD
	 * constructor
	 */
	public static List<Pair<Long, Double>> ZipfDist(int nbTermSet, double param) {
		List<Pair<Long, Double>> res = new ArrayList<>();
		ZipfDistribution dist = new ZipfDistribution(nbTermSet, param);
		for (long i = 1; i <= nbTermSet; i++) {
			res.add(new Pair<Long, Double>(i, dist.probability((int) i)));
		}
		return res;
	}

	public static List<Pair<Long, Double>> UnifDist(int nbTermSet) {
		List<Pair<Long, Double>> res = new ArrayList<>();
		for (long i = 0; i < nbTermSet; i++) {
			res.add(new Pair<Long, Double>(i, 1. / nbTermSet));
		}
		return res;
	}

	/**
	 * Exponential distributed time of next request
	 */
	public static long getNextTime() {
		double lambda = 0.0001;
		return (long) (new ExponentialDistribution(1. / lambda).sample());
	}
	
	/**
	 * generates a List of nbRequest Request.Builders</br>
	 * searchContent, ComponentId, apiId, requestId, dataToTransfer and
	 * ExpectedDuration are set here</br>
	 */
	public static List<Request.Builder> buildersRequests(List<Pair<Long, Double>> termDist) {
		List<Pair<Request.Builder, Double>> requestSet = new ArrayList<>();
		for (int nR = 1; nR <= NB_REQUEST; nR++) {
			Request.Builder requestBuilder = Request.newBuilder();
			requestBuilder.setSearchContent(randQueryContent(termDist, Launcher.randGint(AVG_NWREQ, STD_NWREQ)));
			requestBuilder.setComponentId("1").setApiId("1_1").setRequestId(nR).setDataToTransfer(1)
					.setExpectedDuration(100); // TODO change !

			requestSet.add(
					new Pair<Request.Builder, Double>(requestBuilder, getWeight(requestBuilder.build(), termDist)));
		}

		// Generating distribution
		EnumeratedDistribution<Request.Builder> requestDist = new EnumeratedDistribution<Request.Builder>(requestSet);

		// Picking requests
		List<Request.Builder> requestSequence = Arrays.asList(requestDist.sample(NB_REQUEST, new Request.Builder[] {}));

		return requestSequence;

	}

	

}
