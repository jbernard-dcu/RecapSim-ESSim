package main;

import java.util.List;
import java.util.Random;

import org.apache.commons.math3.util.Pair;

import essim.ESSim;
import essim.Generation;
import eu.recap.sim.models.ApplicationModel.ApplicationLandscape;
import eu.recap.sim.models.ExperimentModel.Experiment;
import eu.recap.sim.models.InfrastructureModel.Infrastructure;
import eu.recap.sim.models.WorkloadModel.*;
import synthetic.Document;
import synthetic.Shard;
import synthetic.SynthUtils;

public class Launcher {

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

		List<Pair<Long, Double>> termDist = SynthUtils.ZipfDist(SynthUtils.NB_TERMSET, 1.);
		// TreeMap<Long, Double> termDist = UnifDist(NB_TERMSET);

		List<Document> data = Generation.GenerateDatabase(termDist, SynthUtils.NB_DOCS, SynthUtils.AVG_NWDOC,
				SynthUtils.STD_NWDOC);

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
		int startr = 0;

		int nbRequest = 200;
		int start = startr;

		Workload workload = Generation.GenerateYCSBWorkload(appLandscape, start, nbRequest);

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

}
