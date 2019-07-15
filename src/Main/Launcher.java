package Main;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import Classes.Document;
import Classes.Generation;
import Classes.Print;
import Classes.Shard;
import Classes.TxtReader;
import Classes.TxtReader.writeOrRead;
import Distribution.*;
import eu.recap.sim.RecapSim;
import eu.recap.sim.models.ApplicationModel.ApplicationLandscape;
import eu.recap.sim.models.ExperimentModel.Experiment;
import eu.recap.sim.models.InfrastructureModel.Infrastructure;
import eu.recap.sim.models.WorkloadModel.*;

public class Launcher {

	// Parameters nbWord request
	final static double AVG_NWREQ = 4.;
	final static double STD_NWREQ = 2.;

	// Parameters nbWord document
	final static int AVG_NWDOC = 10;
	final static int STD_NWDOC = 5;

	// Parameters termSet and querySet
	final static int NB_TERMSET = 10_000;
	final static int NB_REQUEST = 10;

	// Parameters database
	final static int NB_DOCS = 1_000;
	final static int NB_INDEX = 1;
	final static int NB_PRIMARYSHARDS = 3;
	final static int NB_REPLICAS = 3; // per primary shard
	final static int NB_TOTALSHARDS = NB_PRIMARYSHARDS * (1 + NB_REPLICAS);

	// Parameters ApplicationModel
	final static int NB_APPS = 1;

	// Parameters search results
	final static double p_DOC = .2; // We only give the p_DOC*NB_DOCS best documents for results

	public static void main(String[] args) throws Exception {

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// WORKLOAD GENERATION
		///////////////////////////////////////////////////////////////////////////////////////////////

		TreeMap<Long, Double> termDist = BasicZipfDist(NB_TERMSET);
		// Workload workload = Generation.GenerateSyntheticWorkload(termDist,
		// NB_TERMSET, NB_REQUEST);

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// DATABASE GENERATION
		///////////////////////////////////////////////////////////////////////////////////////////////

		List<Document> data = Generation.GenerateDatabase(termDist, NB_DOCS, AVG_NWDOC, STD_NWDOC);

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// INFRASTRUCTURE GENERATION
		///////////////////////////////////////////////////////////////////////////////////////////////

		Infrastructure infrastructure = Generation.GenerateInfrastructure("General Infrastructure");

		List<Shard> shardBase = Generation.GenerateShardBase(infrastructure, NB_TOTALSHARDS, NB_REPLICAS);

		/*
		 * Routing documents to shards
		 */
		long startTime = System.currentTimeMillis();

		for (Document doc : data) {
			int index = (doc.getID() % NB_PRIMARYSHARDS) * (NB_REPLICAS + 1);
			shardBase.get(index).addDocument(doc);
		}

		System.out.println("Routing done:" + (System.currentTimeMillis() - startTime) + "ms");

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// APPLICATION MODEL
		///////////////////////////////////////////////////////////////////////////////////////////////

		ApplicationLandscape appLandscape = Generation.GenerateApplicationLandscape(NB_APPS, NB_PRIMARYSHARDS,
				infrastructure);

		/*
		 * /////////////////////////////////////////////////////////////////////////////
		 * ////////////////// /////////////////// SEARCH RESULTS
		 * /////////////////////////////////////////////////////////////////////////////
		 * //////////////////
		 * /////////////////////////////////////////////////////////////////////////////
		 * //////////////////
		 * 
		 * // List of requests Request request = workload.getDevices(0).getRequests(0);
		 * System.out.println(request.getSearchContent());
		 * 
		 * for (Shard shard : shardBase) { if (shard.isPrimaryShard()) {
		 * System.out.println(
		 * "---------------------------------------------------------");
		 * System.out.println(shard.fetchResults(request, shardBase, p_DOC).toString());
		 * } }
		 * 
		 * // Print.printDocScore(data, shardBase,
		 * workload.getDevices(0).getRequests(0));
		 */

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// EXPERIMENT CONFIGURATION
		///////////////////////////////////////////////////////////////////////////////////////////////

		// Workload workload = TxtReader.GenerateWorkload(3, writeOrRead.R,
		// appLandscape); // Working
		Workload workload = Generation.GenerateSyntheticWorkload(termDist, NB_TERMSET, NB_REQUEST, appLandscape,
				shardBase);

		Experiment.Builder configBuilder = Experiment.newBuilder();
		configBuilder.setName("General config");
		configBuilder.setDuration(200);
		configBuilder.setApplicationLandscape(appLandscape).setInfrastructure(infrastructure).setWorkload(workload);
		Experiment config = configBuilder.build();

		///////////////////////////////////////////////////////////////////////////////////////////////
		/////////////////// RESULTS
		///////////////////////////////////////////////////////////////////////////////////////////////

		launchSimulation(config);

	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////// METHODS
	/////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////

	public static void launchSimulation(Experiment config) {
		RecapSim recapExperiment = new RecapSim();
		String simulationId = recapExperiment.StartSimulation(config);
		System.out.println("Simulation is:" + recapExperiment.SimulationStatus(simulationId));
	}

	/**
	 * Creates a basic Zipfian distribution, useful to create the parameter of FreqD
	 * constructor
	 */
	public static TreeMap<Long, Double> BasicZipfDist(int nbTermSet) {
		TreeMap<Long, Double> res = new TreeMap<Long, Double>();
		for (long i = 1; i <= nbTermSet; i++) {
			res.put(i, 1. / i);
		}
		return res;
	}

	/**
	 * Exponential distributed time of next request
	 */
	public static long getNextTime() {
		double lambda = 0.0001;
		return (long) (new ExpD(lambda).sample());
	}

	/**
	 * generates a List of nbRequest Request.Builders</br>
	 * searchContent, ComponentId, apiId, requestId, dataToTransfer and 
	 * ExpectedDuration are set here</br>
	 * TODO : add as many settings as possible
	 */
	public static List<Request.Builder> buildersRequests(TreeMap<Long, Double> termDist) {
		// Generating RequestSet and RequestScores
		List<Request.Builder> requestSet = new ArrayList<Request.Builder>();
		List<Double> requestScores = new ArrayList<Double>();
		for (int nR = 0; nR < NB_REQUEST; nR++) {
			Request.Builder requestBuilder = Request.newBuilder();
			requestBuilder.setSearchContent(randQueryContent(termDist, randGint(AVG_NWREQ, STD_NWREQ)));
			requestBuilder.setComponentId("1").setApiId("1_1").setRequestId(nR).setDataToTransfer(1)
					.setExpectedDuration(100); // TODO change !
			// TODO requestBuilder.set...
			requestSet.add(requestBuilder);
			requestScores.add(getWeight(requestBuilder.build()));

		}

		// Generating distribution
		FreqD<Request.Builder> dist = new FreqD<Request.Builder>(requestSet, requestScores);

		// Picking requests
		List<Request.Builder> requestSequence = new ArrayList<Request.Builder>();
		for (int req = 0; req < NB_REQUEST; req++) {
			requestSequence.add(dist.sample());
		}

		return requestSequence;

	}

	/**
	 * Returns the score of the query</br>
	 * Change this method to change way of valorising requests.
	 */
	public static double getWeight(Request r) {
		List<Long> content = unparse(r.getSearchContent());
		double score = 0.;
		for (long term : content) {
			score += 1. / term;
		}
		return score / content.size();
	}

	/**
	 * Random integer number with a gaussian distribution</br>
	 * Change parameters or this to have a different distribution of the length of
	 * words
	 */
	public static int randGint(double avg, double std) {
		int res = (int) (avg + std * new Random().nextGaussian());
		return (res <= 0) ? (int) avg : res;
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
