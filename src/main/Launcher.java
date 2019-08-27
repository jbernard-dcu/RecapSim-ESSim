package main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.util.Pair;

import essim.ESSim;
import essim.Generation;
import eu.recap.sim.models.ApplicationModel.ApplicationLandscape;
import eu.recap.sim.models.ExperimentModel.Experiment;
import eu.recap.sim.models.InfrastructureModel.Infrastructure;
import eu.recap.sim.models.WorkloadModel.*;
import synthetic.Document;
import synthetic.Shard;

public class Launcher {

	/*
	 * Instance variables
	 */
	// synthetic workload

	/*
	 * Parameters synthetic workload
	 */
	// Parameters nbWord request
	final static double AVG_NWREQ = 4.;
	final static double STD_NWREQ = 2.;
	// Parameters nbWord document
	final static int AVG_NWDOC = 10;
	final static int STD_NWDOC = 5;
	// Parameters termSet and requestSet
	final static int NB_TERMSET = 10_000;
	public final static int NB_REQUEST = 10;
	// Parameters database
	final static int NB_DOCS = 1_000;

	/*
	 * Common parameters
	 */
	// Parameters database
	// public final static int NB_INDEX = 1;
	public final static int NB_PRIMARYSHARDS = 9;
	public final static int NB_REPLICAS = 3; // per primary shard
	public final static int NB_TOTALSHARDS = NB_PRIMARYSHARDS * (1 + NB_REPLICAS);
	// Parameters ApplicationModel
	final static int NB_APPS = 1;

	public static void main(String[] args) throws Exception {

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// DATABASE GENERATION
		///////////////////////////////////////////////////////////////////////////////////////////////

		List<Pair<Long, Double>> termDist = ZipfDist(NB_TERMSET, 1.);
		// TreeMap<Long, Double> termDist = UnifDist(NB_TERMSET);

		List<Document> data = Generation.GenerateDatabase(termDist, NB_DOCS, AVG_NWDOC, STD_NWDOC);

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// INFRASTRUCTURE GENERATION
		///////////////////////////////////////////////////////////////////////////////////////////////

		Infrastructure infrastructure = Generation.GenerateInfrastructure("General Infrastructure");

		List<Shard> shardBase = Generation.GenerateShardBase(infrastructure, NB_PRIMARYSHARDS, NB_REPLICAS, data);

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// APPLICATION LANDSCAPE
		///////////////////////////////////////////////////////////////////////////////////////////////

		ApplicationLandscape appLandscape = Generation.GenerateAppLandscape(NB_APPS, NB_PRIMARYSHARDS, infrastructure);

		// System.out.println(appLandscape.getApplications(0).getComponentsList().toString());

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// WORKLOAD GENERATION
		///////////////////////////////////////////////////////////////////////////////////////////////

		int startw = 10;
		int startr = 1_500_000;

		int nbRequest = 10;
		int start = startr;

		Workload workload = Generation.GenerateYCSBWorkload(appLandscape, start, nbRequest);
		// Workload workload = Generation.GenerateSyntheticWorkload(termDist,
		// NB_TERMSET, NB_REQUEST, appLandscape,
		// shardBase);

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// RESULTS
		///////////////////////////////////////////////////////////////////////////////////////////////

		launchSimulation(infrastructure, appLandscape, workload);

	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// CONSTRUCTOR
	/////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////

	public static void launchSimulation(Infrastructure rim, ApplicationLandscape ram, Workload rwm) {
		Experiment.Builder configBuilder = Experiment.newBuilder();
		configBuilder.setName("General config");
		configBuilder.setDuration(200);
		configBuilder.setApplicationLandscape(ram).setInfrastructure(rim).setWorkload(rwm);
		Experiment config = configBuilder.build();

		ESSim esExperiment = new ESSim();

		String simulationId = esExperiment.StartSimulation(config);
		System.out.println("Simulation is:" + esExperiment.SimulationStatus(simulationId));
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// METHODS
	/////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////

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
			requestBuilder.setSearchContent(randQueryContent(termDist, randGint(AVG_NWREQ, STD_NWREQ)));
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

	public static int randGint(double avg, double std, int lowerBound, int upperBound) {
		int res = (int) (avg + std * new Random().nextGaussian());
		return (res <= lowerBound) ? lowerBound : (res >= upperBound) ? upperBound : res;
	}

	/**
	 * Random integer number with a gaussian distribution</br>
	 * Change parameters or this to have a different distribution of the length of
	 * words
	 */
	public static int randGint(double avg, double std) {
		return randGint(avg, std, 1, Integer.MAX_VALUE);
	}

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

}
