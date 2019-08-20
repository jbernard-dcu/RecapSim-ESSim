package Classes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import eu.recap.sim.models.ApplicationModel.Application;
import eu.recap.sim.models.ApplicationModel.ApplicationLandscape;
import eu.recap.sim.models.ApplicationModel.Application.Component;
import eu.recap.sim.models.InfrastructureModel.Infrastructure;
import eu.recap.sim.models.InfrastructureModel.Node;
import eu.recap.sim.models.InfrastructureModel.ResourceSite;

public final class OldMethods {

	/**
	 * Generates a new ApplicationLandscape taking into account the difference of
	 * treatment between index and search requests (the dataNodes now have apis
	 * connecting them in case of a index request)
	 */
	public static ApplicationLandscape GenerateAppLandscape3(int appQty, int nbDNs, Infrastructure infrastructure) {
		long startTime = System.currentTimeMillis();

		ApplicationLandscape.Builder applicationLandscapeBuilder = ApplicationLandscape.newBuilder();
		applicationLandscapeBuilder.setNotes("General application landscape");

		// List of available nodes
		List<String> nodeIds = new ArrayList<String>();
		for (ResourceSite site : infrastructure.getSitesList()) {
			for (Node node : site.getNodesList()) {
				nodeIds.add(node.getId());
			}
		}
		int indexNmberOfNodes = nodeIds.size() - 1;

		int nodesCounter = 0;
		for (int appCounter = 0; appCounter < appQty; appCounter++) {

			// New application builder
			Application.Builder appBuilder = Application.newBuilder();
			appBuilder.setApplicationId("" + appCounter).setApplicationName("" + appCounter);

			// Component for WS
			Component.Builder webServerBuilder = createWSComponent(nodeIds.get(nodesCounter));
			nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;
			appBuilder.addComponents(webServerBuilder.build());

			// Component for ES client
			Component.Builder esClientBuilder = createESClientComponent(nodeIds.get(nodesCounter), nbDNs);
			nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;
			appBuilder.addComponents(esClientBuilder.build());

			/*
			 * Creating, deploying and building shards
			 */
			for (int dnId = 3; dnId < 3 + nbDNs; dnId++) {

				Component.Builder dnBuilder = createDNComponent("DN_" + (dnId - 2), "" + dnId,
						nodeIds.get(nodesCounter), nbDNs);

				nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;

				appBuilder.addComponents(dnBuilder.build());
			}

			applicationLandscapeBuilder.addApplications(appBuilder.build());

		}

		System.out.println("ApplicationLandscape generated:" + (System.currentTimeMillis() - startTime) + "ms");

		return applicationLandscapeBuilder.build();
	}

	/**
	 * DN----------DN
	 * | |
	 * DN----...---DN
	 */
	public static ApplicationLandscape GenerateAppLandscape2(int appQty, int nbComponents,
			Infrastructure infrastructure) {

		long startTime = System.currentTimeMillis();

		ApplicationLandscape.Builder appLandscapeBuilder = ApplicationLandscape.newBuilder();
		appLandscapeBuilder.setNotes("Network application landscape");

		// List of available nodes
		List<String> nodeIds = new ArrayList<String>();
		for (ResourceSite site : infrastructure.getSitesList()) {
			for (Node node : site.getNodesList()) {
				nodeIds.add(node.getId());
			}
		}
		int indexNmberOfNodes = nodeIds.size() - 1;

		int nodesCounter = 0;

		for (int appCounter = 0; appCounter < appQty; appCounter++) {

			// New Application builder
			Application.Builder appBuilder = Application.newBuilder();
			appBuilder.setApplicationId("" + appCounter).setApplicationName("" + appCounter);

			// Creating, deploying and building shards
			// "shards" go from 1 to NB_PRIMARYSHARDS. shard is the component id in fact
			// We do not consider replication for now
			for (int componentId = 1; componentId <= nbComponents; componentId++) {

				Component.Builder componentBuilder = createComponent("Shard_" + componentId, "" + componentId,
						nodeIds.get(nodesCounter), nodeIds.size());

				nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;

				appBuilder.addComponents(componentBuilder.build());
			}

			appLandscapeBuilder.addApplications(appBuilder.build());

		}

		System.out.println("ApplicationLandscape generated:" + (System.currentTimeMillis() - startTime) + "ms");

		return appLandscapeBuilder.build();
	}

	/**
	 * Return the join of the two workloads. Values are re-sorted by date</br>
	 * 0=date(long), 1=type(String), 2=latency(double)
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	public static final List<List<Object>> mergeWorkloads2() {
		List<List<Object>> workloadW = getRequests(9, "W");
		List<List<Object>> workloadR = getRequests(9, "R");

		List<List<Object>> workloadMerged = new ArrayList<List<Object>>();
		for (int field = 0; field < 3; field++) {
			workloadMerged.add(new ArrayList<>());
		}

		int sizeMerged = workloadR.get(0).size() + workloadW.get(0).size();

		while (workloadMerged.get(0).size() < sizeMerged) {

			long minR = Collections.min((List<Long>) (List<?>) workloadR.get(0));
			long minW = Collections.min((List<Long>) (List<?>) workloadW.get(0));
			long min = Math.min(minR, minW);

			List<List<Object>> workloadMin = (min == minR) ? workloadR : workloadW;
			int indexMin = workloadMin.get(0).indexOf(min);

			for (int field = 0; field < 3; field++) {
				workloadMerged.get(field).add(workloadMin.get(field).get(indexMin));
			}

			for (int field = 0; field < 3; field++) {
				((min == minR) ? workloadR : workloadW).get(field).remove(indexMin);
			}

		}

		return workloadMerged;

	}

	/**
	 * 0=Date, 1=time, 2=nOp, 3=throughput, 4=estTime, 5=specs
	 */
	@Deprecated
	public static List<List<Object>> getRequests(int numberNodes, String pick) {
		String path = "/elasticsearch_nodes-" + numberNodes + "_replication-3/nodes-" + numberNodes
				+ "_replication-3/evaluation_run_2018_11_25-";
		if (numberNodes == 3)
			path += "19_10/data/";
		if (numberNodes == 9)
			path += "22_40/data/";

		String fileName = "";
		switch (pick) {
		case "W":
			fileName = "load.txt";
			break;
		case "R":
			fileName = "transaction.txt";
		}

		String line = null;
		List<List<Object>> validRequest = new ArrayList<List<Object>>();

		try {
			File file = new File(System.getProperty("user.dir") + File.separator + path + File.separator + fileName);

			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			final int NB_FIELDS = 6;

			for (int field = 0; field < NB_FIELDS; field++) {
				validRequest.add(new ArrayList<Object>());
			}

			while ((line = bufferedReader.readLine()) != null) {

				if (line.startsWith("2018") && !line.contains("Thread")) {

					// Date
					int start = 11;
					int hours = Integer.parseInt(TxtReader.getWord(line, start, ":"));
					int minutes = Integer.parseInt(TxtReader.getWord(line, start + 3, ":"));
					int seconds = Integer.parseInt(TxtReader.getWord(line, start + 6, ":"));
					int milliseconds = Integer.parseInt(TxtReader.getWord(line, start + 9, " "));
					long addDate = milliseconds + 1000 * seconds + 1000 * 60 * minutes + 1000 * 60 * 60 * hours
							+ new Date(2018, 11, 25).getTime();

					// Timestamp
					start = 24;
					int addTime = Integer.parseInt(TxtReader.getWord(line, start, " "));

					// Number of operations
					start = line.indexOf("sec:", start) + 5;
					int addNOp = Integer.parseInt(TxtReader.getWord(line, start, " "));

					// Throughput
					double addThroughput = Double.MAX_VALUE;
					if (line.contains("ops/sec")) {
						start = line.indexOf("operations;", start) + 12;
						addThroughput = Double.parseDouble(TxtReader.getWord(line, start, " "));
					}

					// Estimate time of completion
					long addEstTime = 0;
					if (line.contains("est completion in")) {
						start = line.indexOf("est completion in", start) + 18;
						addEstTime = TxtReader.readTimeWorkload(TxtReader.getWord(line, start, "["));
					}

					// Building the list of wanted request types
					List<String> requestTypes = Arrays.asList("READ", "INSERT", "UPDATE");

					// Request specs and add all
					if (line.contains("[")) {
						start = line.indexOf("[", start) + 1;
						while (start > 0) {
							SpecRequest addSpecs = new SpecRequest(TxtReader.getWord(line, start, "]"));

							if (requestTypes.contains(addSpecs.getType())) {
								validRequest.get(0).add(addDate);
								validRequest.get(1).add(addTime);
								validRequest.get(2).add(addNOp);
								validRequest.get(3).add(addThroughput);
								validRequest.get(4).add(addEstTime);
								validRequest.get(5).add(addSpecs);
							}
							start = line.indexOf("[", start) + 1;
						}
					}

				}
			}

			bufferedReader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return validRequest;
	}

	/**
	 * 0=Date, 1=time, 2=nOp, 3=throughput, 4=estTime, 5=Spec </br>
	 * Merges the workloads of load and transaction for the 9 nodes case, also sorts
	 * all operations according to their date
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	public final static List<List<Object>> mergeWorkloads() {
		/*
		 * Sorting requests from both workloads
		 */
		List<List<Object>> validRequestsW = getRequests(9, "W");
		List<List<Object>> validRequestsR = getRequests(9, "R");

		List<Long> vRWdates = (List<Long>) (List<?>) validRequestsW.get(0);
		List<Long> vRRdates = (List<Long>) (List<?>) validRequestsR.get(0);

		List<List<Object>> validRequests = new ArrayList<List<Object>>();
		for (int field = 0; field < 6; field++) {
			validRequests.add(new ArrayList<Object>());
		}

		int indexWmin;
		int indexRmin;
		int indexMin;
		List<List<Object>> vRmin;
		for (int index = 0; index < vRWdates.size() + vRRdates.size(); index++) {
			indexWmin = vRWdates.indexOf(Collections.min(vRWdates));
			indexRmin = vRRdates.indexOf(Collections.min(vRRdates));

			if (vRWdates.get(indexWmin) >= vRRdates.get(indexRmin)) {
				indexMin = indexRmin;
				vRmin = validRequestsR;
			} else {
				indexMin = indexWmin;
				vRmin = validRequestsW;
			}

			for (int field = 0; field < 6; field++) {
				validRequests.get(field).add(vRmin.get(field).get(indexMin));
			}

			if (vRmin.equals(validRequestsW)) {
				vRWdates.set(indexWmin, Long.MAX_VALUE);
			} else {
				vRRdates.set(indexRmin, Long.MAX_VALUE);
			}

		}

		return validRequests;
	}

}
