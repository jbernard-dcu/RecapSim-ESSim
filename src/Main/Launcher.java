package Main;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.math3.distribution.ExponentialDistribution;

import Classes.Document;
import Classes.Generation;
import Classes.Shard;
import Distribution.*;
import eu.recap.sim.RecapSim;
import eu.recap.sim.models.ApplicationModel.ApplicationLandscape;
import eu.recap.sim.models.ExperimentModel.Experiment;
import eu.recap.sim.models.InfrastructureModel.Infrastructure;
import eu.recap.sim.models.WorkloadModel.*;

public class Launcher {

	/*
	 * Instance variables
	 */
	// synthetic workload

	// common
	private Infrastructure infrastructure;
	private ApplicationLandscape appLandscape;
	private Workload workload;
	private Experiment config;
	private RecapSim recapExperiment;

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

		TreeMap<Long, Double> termDist = ZipfDist(NB_TERMSET, 1.);
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

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// WORKLOAD GENERATION
		///////////////////////////////////////////////////////////////////////////////////////////////

		int start = 83 - 10;
		int nbRequest = 10;

		Workload workload = Generation.GenerateYCSBWorkload(NB_PRIMARYSHARDS, appLandscape, start, nbRequest);
		// Workload workload =
		// Generation.GenerateSyntheticWorkload(termDist,NB_TERMSET,NB_REQUEST,
		// appLandscape, shardBase);

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// RESULTS
		///////////////////////////////////////////////////////////////////////////////////////////////

		new Launcher(infrastructure, appLandscape, workload);

	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// CONSTRUCTOR
	/////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////

	public Launcher(Infrastructure infrastructure, ApplicationLandscape appLandscape, Workload workload) {
		this.infrastructure = infrastructure;

		this.appLandscape = appLandscape;

		this.workload = workload;

		Experiment.Builder configBuilder = Experiment.newBuilder();
		configBuilder.setName("General config");
		configBuilder.setDuration(200);
		configBuilder.setApplicationLandscape(appLandscape).setInfrastructure(infrastructure).setWorkload(workload);
		this.config = configBuilder.build();

		this.recapExperiment = new RecapSim();

		String simulationId = recapExperiment.StartSimulation(config);
		System.out.println("Simulation is:" + recapExperiment.SimulationStatus(simulationId));
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// METHODS
	/////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////

	public Infrastructure getInfrastructure() {
		return infrastructure;
	}

	public void setInfrastructure(Infrastructure infrastructure) {
		this.infrastructure = infrastructure;
	}

	public ApplicationLandscape getAppLandscape() {
		return appLandscape;
	}

	public void setAppLandscape(ApplicationLandscape appLandscape) {
		this.appLandscape = appLandscape;
	}

	public Workload getWorkload() {
		return workload;
	}

	public void setWorkload(Workload workload) {
		this.workload = workload;
	}

	public Experiment getConfig() {
		return config;
	}

	public void setConfig(Experiment config) {
		this.config = config;
	}

	public RecapSim getRecapExperiment() {
		return recapExperiment;
	}

	public void setRecapExperiment(RecapSim recapExperiment) {
		this.recapExperiment = recapExperiment;
	}

	/**
	 * Creates a basic Zipfian distribution, useful to create the parameter of FreqD
	 * constructor
	 */
	public static TreeMap<Long, Double> ZipfDist(int nbTermSet, double param) {
		TreeMap<Long, Double> res = new TreeMap<Long, Double>();
		for (long i = 1; i <= nbTermSet; i++) {
			res.put(i, 1. / Math.pow(i, param));
		}
		return res;
	}

	public static TreeMap<Long, Double> UnifDist(int nbTermSet) {
		TreeMap<Long, Double> res = new TreeMap<Long, Double>();
		for (long i = 0; i < nbTermSet; i++) {
			res.put(i, 1. / nbTermSet);
		}
		return res;
	}

	/**
	 * Exponential distributed time of next request
	 */
	public static long getNextTime() {
		double lambda = 0.0001;
		return (long) (new ExponentialDistribution(1./lambda).sample());
	}

	/**
	 * generates a List of nbRequest Request.Builders</br>
	 * searchContent, ComponentId, apiId, requestId, dataToTransfer and
	 * ExpectedDuration are set here</br>
	 */
	public static List<Request.Builder> buildersRequests(TreeMap<Long, Double> termDist) {
		// Generating RequestSet and RequestScores
		List<Request.Builder> requestSet = new ArrayList<Request.Builder>();
		List<Double> requestScores = new ArrayList<Double>();
		for (int nR = 1; nR <= NB_REQUEST; nR++) {
			Request.Builder requestBuilder = Request.newBuilder();
			requestBuilder.setSearchContent(randQueryContent(termDist, randGint(AVG_NWREQ, STD_NWREQ)));
			requestBuilder.setComponentId("1").setApiId("1_1").setRequestId(nR).setDataToTransfer(1)
					.setExpectedDuration(100); // TODO change !
			requestSet.add(requestBuilder);
			requestScores.add(getWeight(requestBuilder.build(), termDist));

		}

		// Generating distribution
		FreqD<Request.Builder> requestDist = new FreqD<Request.Builder>(requestSet, requestScores);

		// Picking requests
		List<Request.Builder> requestSequence = new ArrayList<Request.Builder>();
		for (int req = 0; req < NB_REQUEST; req++) {
			requestSequence.add(requestDist.sample());
		}

		return requestSet;
		// return requestSequence;

	}

	/**
	 * Returns the weight of the query</br>
	 * Change this method to change way of valorising requests.
	 */
	public static double getWeight(Request r, TreeMap<Long, Double> termDist) {
		List<Long> content = unparse(r.getSearchContent());
		double score = 0.;
		for (long term : content) {
			score += termDist.get(term);
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
		return randGint(avg, std, 0, Integer.MAX_VALUE);
	}

	/**
	 * Creates a formatted String giving the contents of a Request. </br>
	 * Change this method to change the format of the String.</br>
	 * Change proto file of WorkloadModel to change type of request content. </br>
	 */
	public static String randQueryContent(TreeMap<Long, Double> termDist, int nbWord) {
		// Creating distribution
		FreqD<Long> dist = new FreqD<Long>(termDist);

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
