package essim;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.util.Pair;

import essim.Parameters.path;
import essim.Parameters.typeParam;
import eu.recap.sim.models.ApplicationModel.Application;
import eu.recap.sim.models.ApplicationModel.ApplicationLandscape;
import eu.recap.sim.models.ApplicationModel.Deployment;
import eu.recap.sim.models.ApplicationModel.VeFlavour;
import eu.recap.sim.models.ApplicationModel.Application.Component;
import eu.recap.sim.models.InfrastructureModel.Infrastructure;
import eu.recap.sim.models.InfrastructureModel.Link;
import eu.recap.sim.models.InfrastructureModel.Node;
import eu.recap.sim.models.InfrastructureModel.ResourceSite;
import eu.recap.sim.models.InfrastructureModel.Node.CPU;
import eu.recap.sim.models.InfrastructureModel.Node.Core;
import eu.recap.sim.models.InfrastructureModel.Node.Memory;
import eu.recap.sim.models.InfrastructureModel.Node.Storage;
import eu.recap.sim.models.InfrastructureModel.ResourceSite.SiteLevel;
import eu.recap.sim.models.LocationModel.Location;
import eu.recap.sim.models.WorkloadModel.Device;
import eu.recap.sim.models.WorkloadModel.Request;
import eu.recap.sim.models.WorkloadModel.Workload;
import main.Launcher;
import synthetic.Document;
import synthetic.Shard;
import synthetic.SynthUtils;
import utils.Print;
import utils.TxtUtils;
import utils.WorkloadReader;
import utils.TxtUtils.loadMode;
import utils.TxtUtils.typeData;

/**
 * This class holds useful methods to generate the objects necessary for the
 * modelisation
 */
public final class Generation {

	/**
	 * Divide by this value to convert time in millisconds to time in seconds
	 */
	static int timeUnits = 1000;

	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////// PARAMETERS
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////

	/* Sites */
	static final int numberSites = 1;
	static int nbDataNodes = Launcher.NB_PRIMARYSHARDS;
	static int[] numberNodesPerSite = { 2 + nbDataNodes };// WS + ES + Data nodes

	// Application landscape
	// VM memory and storage expressed in Mo
	// all VMs are the same

	// Ve flavours
	// 2, 4000, 70000
	static final int vmCores = 2;
	static final int vmMemory = 4_000; // 5,000 in output
	static final int vmStorage = 70_000;
	/* ES client */
	static final int esClient_cores = 8;
	static final int esClient_memory = 8_000;
	static final int esClient_storage = 20_000;

	// Hosts
	static int[][] cpuFrequency = initSameValue(numberSites, numberNodesPerSite, 3000); // MIPS or 2.6 GHz
	static int[][] cpuCores = initSameValue(numberSites, numberNodesPerSite, 80);
	static int[][] ram = initSameValue(numberSites, numberNodesPerSite, (int) 1E12); // host memory (MEGABYTE)
	static int[][] hdd = initSameValue(numberSites, numberNodesPerSite, (int) 1E9); // host storage (MEGABYTE)
	static int bw = 10_000; // 10Gbit/s

	// Repartition of requests between data nodes for CPU values
	static double[] repartNodes = TxtUtils.calculateRepartNodes(nbDataNodes, typeData.CpuLoad);

	int randNbDataNodesPerRequest = Launcher.randGint(nbDataNodes / 2., nbDataNodes / 6., 0, nbDataNodes);
	static int nbDataNodesPerRequest = 1 + Launcher.NB_REPLICAS;

	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////// GENERATORS
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////

	public static List<Shard> GenerateShardBase(Infrastructure infrastructure, int NB_PRIMARYSHARDS, int NB_REPLICAS,
			List<Document> data) {
		long startTime = System.currentTimeMillis();

		// Fetching list of nodes
		List<Node> nodes = new ArrayList<Node>();
		for (ResourceSite site : infrastructure.getSitesList()) {
			nodes.addAll(site.getNodesList());
		}

		// Creating shards
		List<Shard> shardBase = new ArrayList<Shard>();
		int nodeId;
		for (int shardId = 0; shardId < NB_PRIMARYSHARDS * (1 + NB_REPLICAS); shardId++) {
			nodeId = shardAllocation(shardId, nodes.size());
			shardBase.add(new Shard(nodes.get(nodeId), nodeId));
		}
		for (int indexShard = 0; indexShard < NB_PRIMARYSHARDS * (1 + NB_REPLICAS); indexShard++) {
			int previousPrimaryShard = (NB_REPLICAS + 1) * (int) (indexShard / (NB_REPLICAS + 1));
			shardBase.get(indexShard).setReplicationGroup(
					shardBase.subList(previousPrimaryShard, previousPrimaryShard + NB_REPLICAS + 1));
			shardBase.get(indexShard).setPrimaryShard(indexShard % (NB_REPLICAS + 1) == 0);
		}

		// Routing documents to shards
		for (Document doc : data) {
			int index = (doc.getID() % NB_PRIMARYSHARDS) * (NB_REPLICAS + 1);
			shardBase.get(index).addDocument(doc);
		}

		System.out.println("Shardbase generated:" + (System.currentTimeMillis() - startTime) + "ms");

		return shardBase;
	}

	/**
	 * Returns a list of Documents with the specified termDist TODO optimize and
	 * more realistic model for file characteristics
	 */
	public static List<Document> GenerateDatabase(List<Pair<Long, Double>> termDist, int NB_DOCS, double AVG_NWDOC,
			double STD_NWDOC) {
		long startTime = System.currentTimeMillis();

		// Generating random length and size
		long maxSize = 1_000_000_000; // bytes

		// Generating distribution of words
		EnumeratedDistribution<Long> wordDist = new EnumeratedDistribution<Long>(termDist);

		List<Document> database = new ArrayList<Document>();
		int nbWord;
		List<Long> docContent;
		for (int doc = 0; doc < NB_DOCS; doc++) {

			// adding content in the document
			nbWord = Launcher.randGint(AVG_NWDOC, STD_NWDOC);
			docContent = Arrays.asList(wordDist.sample(nbWord, new Long[] {}));

			// adding new document to data
			database.add(new Document(docContent, (long) maxSize / (doc + 1), doc));
		}

		System.out.println("Database generated:" + (System.currentTimeMillis() - startTime) + "ms");

		return database;
	}

	/**
	 * Method to generate a basic synthetic workload based on stochastic
	 * distributions</br>
	 * Change (for now) this method to change workload distributions
	 */
	public static Workload GenerateSyntheticWorkload(List<Pair<Long, Double>> termDist, int NB_TERMSET, int NB_REQUEST,
			ApplicationLandscape appLandscape, List<Shard> shardBase) {

		long startTime = System.currentTimeMillis();

		/*
		 * Creation of timeSequence, Exponential distribution
		 */
		List<Long> timeSequence = new ArrayList<Long>();
		timeSequence.add(startTime);
		for (int rang = 1; rang < NB_TERMSET; rang++) {
			timeSequence.add(timeSequence.get(rang - 1) + SynthUtils.getNextTime());
		}

		/*
		 * Generating client IDs list and repart of requests between clients
		 */

		// Creating lists
		List<Integer> freeSpace = new ArrayList<Integer>();
		for (int i = 0; i < NB_REQUEST; i++) {
			freeSpace.add(i);
		}

		// Creating Exp law
		double lambda = 10. / NB_REQUEST;
		ExponentialDistribution exp = new ExponentialDistribution(1. / lambda);

		// Filling clientIds list
		String clientID;
		long idLong = 0;
		String[] IDs = new String[NB_REQUEST]; // Each index corresponds to one request, the value is the clientID
												// corresponding
		while (!freeSpace.isEmpty()) {
			int space = freeSpace.get(0);
			clientID = Long.toString(idLong);
			idLong++;

			while (space < NB_REQUEST) {
				IDs[space] = clientID;
				freeSpace.remove((Integer) space);
				space += (int) exp.sample();
			}
		}

		/*
		 * Creation of Requests
		 */
		List<Request.Builder> buildersRequests = SynthUtils.buildersRequests(termDist);
		List<Request> requests = new ArrayList<Request>();
		int time = 0;
		// request.time and request.applicationId are set here
		for (Request.Builder request : buildersRequests) {
			request.setTime(timeSequence.get(time) - startTime);
			request.setApplicationId(appLandscape.getApplicationsList().get(0).getApplicationId());

			// TODO MIPS data node should depend on where the request is sent
			int mi = (int) Parameters.getParam(typeParam.mips, path.DNToES);
			request.setMipsDataNodes(mi);

			/*
			 * Data nodes destinations for the request
			 */
			List<Long> searchContent = SynthUtils.unparse(request.getSearchContent());

			System.out.println("-------------------------------------------------");
			System.out.println("Search content:" + searchContent.toString());

			List<Shard> shardDist = new ArrayList<Shard>();
			for (long word : searchContent) {
				System.out.println("Word:" + word);
				for (Shard shard : shardBase) {
					if (shard.isPrimaryShard() && shard.getInvertedIndex().containsKey(word)
							&& !shardDist.contains(shard)) {
						System.out.println("> " + shard.toString());
						shardDist.add(shard);
					}
				}
			}

			System.out.println("shardDist:" + shardDist.toString());

			List<Integer> destinationNodes = request.getDataNodesList();
			for (Shard dest : shardDist) {
				int add = Integer.valueOf(dest.getNode().getId().substring(2)) - 1; // counting starts at 1
				if (!destinationNodes.contains(add))
					request.addDataNodes(add);
			}

			System.out.println("Nodes:" + request.getDataNodesList().toString());

			requests.add(request.build());

			time++;
		}

		/*
		 * Allocation of requests on devices, LinknovateValidationRWM_LogAccess TODO :
		 * check
		 */
		// creating devices list
		int deviceQty = 0;
		HashMap<String, Device.Builder> devices = new HashMap<String, Device.Builder>();
		for (String id : IDs) {
			if (!devices.containsKey(id)) {
				Device.Builder device = Device.newBuilder();
				device.setDeviceId(deviceQty + "");
				device.setDeviceName("IP_" + id + "_" + deviceQty);
				deviceQty++;
				devices.put(id, device);
			}
		}
		// adding requests to each device
		for (int req = 0; req < NB_REQUEST; req++) {
			devices.get(IDs[req]).addRequests(requests.get(req));
		}

		/*
		 * Workload generation
		 */
		Workload.Builder workloadBuilder = Workload.newBuilder();
		for (Device.Builder device : devices.values()) {
			workloadBuilder.addDevices(device);
		}

		System.out.println("Workload generated:" + (System.currentTimeMillis() - startTime) + "ms");

		return workloadBuilder.build();
	}

	/**
	 * Method to generate a workload from the input data (3/9 nodes).
	 * 
	 * @param nbRequest : set to negative or 0 to use full workload, else the
	 *                  workload is reduced to <code>nbRequest</code> requests
	 * @throws InterruptedException
	 */
	public static Workload GenerateYCSBWorkload(ApplicationLandscape appLandscape, int start, int nbRequest)
			throws FileNotFoundException, InterruptedException {

		long startTime = System.currentTimeMillis();

		// Depending on the value of nbRequest, we can in fact end up with less than nbRequest requests
		WorkloadReader wReaderW = WorkloadReader.create(nbDataNodes, loadMode.WRITE, nbRequest/2);
		WorkloadReader wReaderR = WorkloadReader.create(nbDataNodes, loadMode.READ, nbRequest/2);

		List<List<Object>> validRequest = TxtUtils.mergeWorkloadsData(wReaderW, wReaderR);

		// Calculating frequency distribution
		List<Integer> nodeIds = new ArrayList<Integer>();
		for (int nodeId = 1; nodeId <= nbDataNodes; nodeId++) {
			nodeIds.add(nodeId);
		}

		List<Pair<Integer, Double>> listValues = new ArrayList<>();
		for (int nodeId : nodeIds) {
			listValues.add(new Pair<Integer, Double>(nodeId, repartNodes[nodeId - 1]));
		}
		EnumeratedDistribution<Integer> nodeDist = new EnumeratedDistribution<Integer>(listValues);

		// Getting starting time of the requests
		long dateInitialRequest = (long) validRequest.get(0).get(0);

		// Clock parameters
		final double cpuFreq = cpuFrequency[0][0] * 1E6; // Hz

		// Adding requests to device
		Device.Builder device = Device.newBuilder();
		for (int request = 0; request < validRequest.get(0).size(); request++) {

			long date = (long) validRequest.get(0).get(request);
			String type = (String) validRequest.get(1).get(request);
			double latency = (double) validRequest.get(2).get(request);

			Request.Builder requestBuilder = Request.newBuilder();

			requestBuilder.setRequestId(request + 1);
			requestBuilder.setComponentId("1");
			requestBuilder.setApiId("1_1");
			// TODO multiple applications
			requestBuilder.setApplicationId(appLandscape.getApplications(0).getApplicationId());
			requestBuilder.setTime(date - dateInitialRequest);// in milliseconds
			requestBuilder.setType(type);

			// Adding destination nodes
			HashSet<Integer> dataNodeDestination = new HashSet<Integer>();
			while (dataNodeDestination.size() < nbDataNodesPerRequest) {
				dataNodeDestination.add(nodeDist.sample());
			}
			requestBuilder.addAllDataNodes(dataNodeDestination);

			// TODO calculate data to transfer

			requestBuilder.setDataToTransfer(1);

			// we don't have expectedDuration for this workload
			requestBuilder.setExpectedDuration(1);

			// MI = duration(s)*freq(Hz)/1E6
			int duration = Math.max((int) (latency / 1_000), 1);
			int miPerDataNode = (int) (cpuFreq * 1E-6 * duration / 1_000);
			requestBuilder.setMipsDataNodes(miPerDataNode * timeUnits);

			device.addRequests(requestBuilder.build());
		}

		// Adding device to workload
		Workload.Builder workload = Workload.newBuilder();
		workload.addDevices(device.build());

		System.out.println("Workload generated:" + (System.currentTimeMillis() - startTime) + "ms");

		// return workload
		return workload.build();

	}

	/**
	 * Method to generate a general ApplicationLandscape, based on
	 * LinknovateValidarionRAM.GenerateLinknovateValidationApplication (@author
	 * Malika)</br>
	 */
	public static ApplicationLandscape GenerateAppLandscape(int appQty, int nbDNs, Infrastructure infrastructure) {

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

			// Component for ES Client
			Component.Builder esClientBuilder = createESClientComponent(nodeIds.get(nodesCounter), nbDNs);
			nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;
			appBuilder.addComponents(esClientBuilder.build());

			/*
			 * Creating, deploying and building shards
			 */
			for (int dnId = 3; dnId < 3 + nbDNs; dnId++) {

				// Topology #1 : non-interconnected datanodes
				Component.Builder dnBuilder = createShardComponent("Shard_" + (dnId - 2), "" + dnId,
						nodeIds.get(nodesCounter));

				// Topology #2 : interconnected datanodes
				// Component.Builder dnBuilder = createDNComponent("DN_" + (dnId - 2), "" +
				// dnId,
				// nodeIds.get(nodesCounter), nbDNs);

				nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;

				appBuilder.addComponents(dnBuilder.build());
			}

			applicationLandscapeBuilder.addApplications(appBuilder.build());

		}

		System.out.println("ApplicationLandscape generated:" + (System.currentTimeMillis() - startTime) + "ms");

		return applicationLandscapeBuilder.build();
	}

	/**
	 * Generates a general infrastructure based on the parameters specified in the
	 * class</br>
	 * Based on ExperimentHelpers.GenerateLinknovateInfrastructure (@author Serguei
	 * Svorobej)
	 */
	public static Infrastructure GenerateInfrastructure(String name) {
		long startTime = System.currentTimeMillis();

		Infrastructure.Builder infrastructure = Infrastructure.newBuilder();
		infrastructure.setName(name);

		// only one link where all sites are connected
		// TODO multiple links and sites
		Link.Builder link = Link.newBuilder();
		link.setId("0");
		link.setBandwith(bw);

		// create sites
		for (int nSite = 0; nSite < numberSites; nSite++) {

			ResourceSite.Builder site = ResourceSite.newBuilder();
			site.setName("Site_" + nSite);
			site.setId(nSite + "");

			Location.Builder geolocation = Location.newBuilder();
			geolocation.setLatitude(nSite);
			geolocation.setLongitude(nSite);

			site.setLocation(geolocation.build());
			site.setHierarchyLevel(SiteLevel.Edge);

			// create nodes
			for (int nNode = 0; nNode < numberNodesPerSite[nSite]; nNode++) {

				Node.Builder node = Node.newBuilder();
				node.setName("Node_" + nSite + "_" + nNode);
				node.setId(nSite + "_" + nNode);

				// TODO parameters cpu builder
				CPU.Builder cpu = CPU.newBuilder();
				cpu.setName("Xeon_" + nSite + "_" + nNode);
				cpu.setId(nSite + "_" + nNode);
				cpu.setMake("Intel");
				cpu.setRating("12345");

				cpu.setFrequency(cpuFrequency[nSite][nNode]);
				// create cores
				for (int e = 0; e < cpuCores[nSite][nNode]; e++) {
					Core.Builder core = Core.newBuilder();
					core.setId(nSite + "_" + nNode + "_" + e);
					cpu.addCpuCores(core.build());
				}

				Memory.Builder memory = Memory.newBuilder();
				memory.setId(nSite + "_" + nNode);
				memory.setCapacity(ram[nSite][nNode]);

				Storage.Builder storage = Storage.newBuilder();
				storage.setId(nSite + "_" + nNode);
				storage.setSize(hdd[nSite][nNode]);

				// add resources to node
				node.addProcessingUnits(cpu.build());
				node.addMemoryUnits(memory.build());
				node.addStorageUnits(storage.build());

				// add node to site
				site.addNodes(node.build());
			}
			ResourceSite builtSite = site.build();
			// add sites to infrastructure
			infrastructure.addSites(builtSite);

			// add sites to link by id
			link.addConnectedSites(builtSite);

		}

		infrastructure.addLinks(link.build());

		System.out.println("Infrastructure generated:" + (System.currentTimeMillis() - startTime) + "ms");

		return infrastructure.build();

	}

	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////// METHODS
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Method returning the int nodeId where the shard should be allocated based on
	 * the allocation policy TODO : in the future, change this method to get an
	 * automatic realistic shard allocation policy
	 */
	private static int shardAllocation(int shardId, int NB_NODES) {
		return 2 + shardId % (NB_NODES - 2);
	}

	/**
	 * Generates an array containing the same value everywhere, for testing/<br>
	 * nbSites*nbNodesPerSite
	 */
	private static int[][] initSameValue(int nbSites, int[] nbNodesPerSite, int value) {
		int[][] res = new int[nbSites][];
		int[] columnRes;
		for (int site = 0; site < nbSites; site++) {
			columnRes = new int[nbNodesPerSite[site]];
			for (int node = 0; node < nbNodesPerSite[site]; node++) {
				columnRes[node] = value;
			}
			res[site] = columnRes;
		}
		return res;
	}

	static Component.Builder createWSComponent(String nodeId) {

		// Component for WS
		Component.Builder webServerBuilder = Component.newBuilder();
		webServerBuilder.setComponentName("Web server").setComponentId("1").setIsLoadbalanced(false);

		// Deployment for WS
		Deployment.Builder deployment_webServer = Deployment.newBuilder();
		deployment_webServer.setNodeId(nodeId);
		webServerBuilder.setDeployment(deployment_webServer.build());

		// Api paths WS
		// path 1
		String apiId = "1_1"; // Component ID, API ID
		Component.Api.Builder webServerApi_1 = Component.Api.newBuilder();
		webServerApi_1.setApiId(apiId);
		webServerApi_1.setApiName(webServerBuilder.getComponentName() + "_" + apiId);
		// resource consumption
		webServerApi_1 = Parameters.setParamsApi(webServerApi_1, path.clientToWS);
		// connect to next api
		webServerApi_1.addNextComponentId("2");
		webServerApi_1.addNextApiId("2_1");
		webServerBuilder.addApis(webServerApi_1.build());

		// path 2
		apiId = "1_2"; // Component ID, API ID
		Component.Api.Builder webServerApi_2 = Component.Api.newBuilder();
		webServerApi_2.setApiId(apiId);
		webServerApi_2.setApiName(webServerBuilder.getComponentName() + "_" + apiId);
		// resource consumption
		webServerApi_2 = Parameters.setParamsApi(webServerApi_2, path.ESToWS);
		// connect to next api
		// no add of next component
		// no add of next api
		webServerBuilder.addApis(webServerApi_2.build());

		// create and add flavour
		VeFlavour.Builder veFlavour_controlPlane = VeFlavour.newBuilder();
		veFlavour_controlPlane.setCores(vmCores);
		veFlavour_controlPlane.setMemory(vmMemory);
		veFlavour_controlPlane.setStorage(vmStorage);
		webServerBuilder.setFlavour(veFlavour_controlPlane.build());

		return webServerBuilder;
	}

	static Component.Builder createESClientComponent(String nodeId, int nbDNs) {

		/*
		 * Component for ES client
		 */
		Component.Builder esClientBuilder = Component.newBuilder();
		esClientBuilder.setComponentName("ES Client").setComponentId("2").setIsLoadbalanced(false);

		/*
		 * Deployment for ES client
		 */
		Deployment.Builder deployment_esClient = Deployment.newBuilder();
		deployment_esClient.setNodeId(nodeId);
		esClientBuilder.setDeployment(deployment_esClient.build());

		/*
		 * Api paths VM2
		 */
		// path 1
		String apiId = "2_1"; // Component ID, API ID
		Component.Api.Builder esClientApi_1 = Component.Api.newBuilder();
		esClientApi_1.setApiId(apiId);
		esClientApi_1.setApiName(esClientBuilder.getComponentName() + "_" + apiId);
		// resource consumption
		esClientApi_1 = Parameters.setParamsApi(esClientApi_1, path.WSToES);
		// connect to next api
		for (int shard = 3; shard < 3 + nbDNs; shard++) {
			esClientApi_1.addNextComponentId("" + shard);
			esClientApi_1.addNextApiId(shard + "_1");
		}

		esClientBuilder.addApis(esClientApi_1.build());

		// path 2
		apiId = "2_2"; // Component ID, API ID
		Component.Api.Builder esClientApi_7 = Component.Api.newBuilder();
		esClientApi_7.setApiId(apiId);
		esClientApi_7.setApiName(esClientBuilder.getComponentName() + "_" + apiId);
		// resource consumption
		esClientApi_7 = Parameters.setParamsApi(esClientApi_7, path.DNToES);
		// connect to next api
		esClientApi_7.addNextComponentId("1");
		esClientApi_7.addNextApiId("1_2");
		esClientBuilder.addApis(esClientApi_7.build());

		// create flavour
		VeFlavour.Builder veFlavour_esClient = VeFlavour.newBuilder();
		veFlavour_esClient.setCores(esClient_cores);
		veFlavour_esClient.setMemory(esClient_memory);
		veFlavour_esClient.setStorage(esClient_storage);

		esClientBuilder.setFlavour(veFlavour_esClient.build());

		return esClientBuilder;
	}

	static Component.Builder createDNComponent(String componentName, String componentId, String nodeId, int nbDNs) {

		// Component for DN
		Component.Builder dnBuilder = Component.newBuilder();
		dnBuilder.setComponentName(componentName).setComponentId(componentId).setIsLoadbalanced(false);

		// Deployment for DN
		Deployment.Builder deployment_shard = Deployment.newBuilder();
		deployment_shard.setNodeId(nodeId);
		dnBuilder.setDeployment(deployment_shard.build());

		// Api paths
		// path 1 : from ES to datanode
		String apiId = componentId + "_1";
		Component.Api.Builder dnApiBuilder_1 = Component.Api.newBuilder();
		dnApiBuilder_1.setApiId(apiId);
		dnApiBuilder_1.setApiName(componentName + "_" + apiId);
		// resource consumption
		dnApiBuilder_1 = Parameters.setParamsApi(dnApiBuilder_1, path.ESToDN);
		// connect to next api
		/*
		 * From a DataNode, the next path is either going back to ES (search or failed
		 * request) or following the request to other datanodes (index request)
		 */
		// adding ES as next component
		dnApiBuilder_1.addNextComponentId("2");
		dnApiBuilder_1.addNextApiId("2_2");
		// adding all other dataNodes as next components
		for (int compId = 3; compId < 3 + nbDNs; compId++) {
			// We consider that for index requests the path from each datanode to itself is
			// not necessary
			if (componentId.contentEquals("" + compId))
				continue;

			dnApiBuilder_1.addNextComponentId("" + compId);
			// arrivingComponent_2_startingComponent for next apis
			dnApiBuilder_1.addNextApiId(compId + "_2_" + componentId);
		}
		dnBuilder.addApis(dnApiBuilder_1.build());

		// path 2 : from other datanodes to this datanode
		for (int compId = 3; compId < 3 + nbDNs; compId++) {
			if (componentId.contentEquals("" + compId))
				continue;

			apiId = componentId + "_2_" + compId;
			Component.Api.Builder dnApiBuilder_2 = Component.Api.newBuilder();
			dnApiBuilder_2.setApiId(apiId);
			dnApiBuilder_2.setApiName(componentName + "_" + apiId);
			// resource consumption
			dnApiBuilder_2 = Parameters.setParamsApi(dnApiBuilder_2, path.ESToDN);
			// adding ES as next component
			dnApiBuilder_2.addNextComponentId("2");
			dnApiBuilder_2.addNextApiId("2_2");

			dnBuilder.addApis(dnApiBuilder_2.build());

		}

		// create flavour
		VeFlavour.Builder veFlavour_dn = VeFlavour.newBuilder();
		veFlavour_dn.setCores(vmCores);
		veFlavour_dn.setMemory(vmMemory);
		veFlavour_dn.setStorage(vmStorage);
		dnBuilder.setFlavour(veFlavour_dn.build());

		return dnBuilder;
	}

	private static Component.Builder createShardComponent(String componentName, String componentId, String nodeId) {
		Component.Builder shard = Component.newBuilder();
		shard.setComponentName(componentName);
		shard.setComponentId(componentId);
		shard.setIsLoadbalanced(false);

		// deploy on consecutive nodes
		Deployment.Builder deployment_shard = Deployment.newBuilder();
		deployment_shard.setNodeId(nodeId);

		shard.setDeployment(deployment_shard.build());

		// create 1 API
		Component.Api.Builder shardApi_1 = Component.Api.newBuilder();
		shardApi_1.setApiId(componentId + "_1");
		shardApi_1.setApiName(shard.getComponentName() + "_" + componentId + "_1");
		// resource consumption
		shardApi_1 = Parameters.setParamsApi(shardApi_1, path.ESToDN);
		// connect to next api TO-DO
		shardApi_1.addNextComponentId("2");
		shardApi_1.addNextApiId("2_2");
		shard.addApis(shardApi_1.build());

		// create flavour
		VeFlavour.Builder veFlavour_shard = VeFlavour.newBuilder();
		veFlavour_shard.setCores(vmCores);
		veFlavour_shard.setMemory(vmMemory);
		veFlavour_shard.setStorage(vmStorage);
		shard.setFlavour(veFlavour_shard.build());

		return shard;
	}
}