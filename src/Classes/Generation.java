package Classes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import Distribution.FreqD;
import Distribution.ExpD;
import Main.Launcher;
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

/**
 * This class holds useful methods to generate the objects necessary for the
 * modelisation</br>
 * TODO take files as parameters to set the constants, especially the hosts
 * parameters
 */
public final class Generation {

	static int timeUnits = 1000;

	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////// PARAMETERS
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////

	// Sites
	static final int numberSites = 1;
	static int[] numberNodesPerSites = { 3 };

	// Hosts
	static final int[][] cpuFrequency = initSameValue(numberSites, numberNodesPerSites, 3000); // MIPS or 2.6 GHz
	static final int[][] cpuCores = initSameValue(numberSites, numberNodesPerSites, 80);
	static final int[][] ram = initSameValue(numberSites, numberNodesPerSites, 2048_000); // host memory (MEGABYTE)
	static final int[][] hdd = initSameValue(numberSites, numberNodesPerSites, 1000000_000); // host storage (MEGABYTE)
	static final int bw = 10_000; // in 10Gbit/s

	// Application landscape
	final static int vmCores = 8;
	final static int vmMemory = 28_000;
	final static int vmStorage = 1_081_000; // all VMs are the same, TODO allow different configurations

	// resource consumption going from client to web server
	static int clientToWebServer_mips = 300 * timeUnits / 10;
	static int clientToWebServer_iops = 1;
	static int clientToWebServer_ram = 200;// 500
	static int clientToWebServer_transferData = 1 * timeUnits;
	// resource consumption going from web server to ES
	static int webServerToES_mips = 300 * timeUnits / 10;
	static int webServerToES_iops = 1;
	static int webServerToES_ram = 200;// 500
	static int webServerToES_transferData = 1 * timeUnits;
	// resource consumption going from ES to DataNode
	static int ESToDataNode_mips = 1 * timeUnits / 10;
	static int ESToDataNode_iops = 1;
	static int ESToDataNode_ram = 200; // 2000
	static int ESToDataNode_transferData = 1 * timeUnits;
	// resource consumption going from DataNode to ES
	static int DataNodeToES_mips = 300 * timeUnits / 10;
	static int DataNodeToES_iops = 1;
	static int DataNodeToES_ram = 200;// 1000
	static int DataNodeToES_transferData = 1 * timeUnits;
	// resource consumption going from ES to Web Server
	static int ESToWebServer_mips = 300 * timeUnits / 10;
	static int ESToWebServer_iops = 1;
	static int ESToWebServer_ram = 200;// 500
	static int ESToWebServer_transferData = 1 * timeUnits;

	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////// GENERATORS
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////

	public static List<Shard> GenerateShardBase(Infrastructure infrastructure, int NB_TOTALSHARDS, int NB_REPLICAS) {
		long startTime = System.currentTimeMillis();

		// Fetching list of nodes
		int NB_NODES = 0;
		List<Node> nodes = new ArrayList<Node>();
		for (ResourceSite site : infrastructure.getSitesList()) {
			nodes.addAll(site.getNodesList());
			NB_NODES += site.getNodesCount();
		}

		// Creating shards
		List<Shard> shardBase = new ArrayList<Shard>();
		int nodeId;
		for (int shardId = 0; shardId < NB_TOTALSHARDS; shardId++) {
			nodeId = shardId % NB_NODES;
			shardBase.add(new Shard(nodes.get(nodeId), shardId));
		}
		for (int indexShard = 0; indexShard < NB_TOTALSHARDS; indexShard++) {
			int previousPrimaryShard = (NB_REPLICAS + 1) * (int) (indexShard / (NB_REPLICAS + 1));
			shardBase.get(indexShard).setReplicationGroup(
					shardBase.subList(previousPrimaryShard, previousPrimaryShard + NB_REPLICAS + 1));
			shardBase.get(indexShard).setPrimaryShard(indexShard % (NB_REPLICAS + 1) == 0);
		}

		System.out.println("Shardbase generated:" + (System.currentTimeMillis() - startTime));

		return shardBase;
	}

	/**
	 * Returns a list of Documents with the specified termDist
	 */
	public static List<Document> GenerateDatabase(TreeMap<Long, Double> termDist, int NB_DOCS, double AVG_NWDOC,
			double STD_NWDOC) {
		long startTime = System.currentTimeMillis();

		/*
		 * Generate list of documents stored on the database
		 */
		// Generating random length and size
		// TODO more realistic model for file sizes and link with number of words
		long maxSize = 1_000_000_000; // bytes

		// Generating distribution of words
		// TODO use different dictionaries
		// TODO optimize, the doc database creation takes too long, complexity
		// NB_DOCS*AVG_NWDOC
		FreqD<Long> wordDist = new FreqD<Long>(termDist);

		List<Document> database = new ArrayList<Document>();
		int nbWord;
		List<Long> docContent;
		for (int doc = 0; doc < NB_DOCS; doc++) {

			// adding content in the document
			nbWord = Launcher.randGint(AVG_NWDOC, STD_NWDOC);
			docContent = new ArrayList<Long>();
			for (int word = 0; word < nbWord; word++) {
				docContent.add(wordDist.sample());
			}

			// adding new document to data
			database.add(new Document(docContent, (long) maxSize / (doc + 1), doc));

		}

		System.out.println("Database generated:" + (System.currentTimeMillis() - startTime));

		return database;
	}

	/**
	 * Method to generate a basic synthetic workload based on stochastic
	 * distributions</br>
	 * Change (for now) this method to change workload distributions
	 */
	public static Workload GenerateSyntheticWorkload(TreeMap<Long, Double> termDist, int NB_TERMSET, int NB_REQUEST,
			ApplicationLandscape appLandscape) {

		long startTime = System.currentTimeMillis();

		/*
		 * Creation of timeSequence, Exponential distribution
		 */
		List<Long> timeSequence = new ArrayList<Long>();
		timeSequence.add(startTime);
		for (int rang = 1; rang < NB_TERMSET; rang++) {
			timeSequence.add(timeSequence.get(rang - 1) + Launcher.getNextTime());
		}

		/*
		 * Generating client IDs list and repart of requests between clients TODO :
		 * allow any distribution
		 */

		// Creating lists
		String[] IDs = new String[NB_REQUEST]; // Each index corresponds to one request, the value is the clientID
												// corresponding
		List<Integer> freeSpace = new ArrayList<Integer>();
		for (int i = 0; i < NB_REQUEST; i++) {
			freeSpace.add(i);
		}
		String clientID;

		// Creating Exp law
		double lambda = 10. / NB_REQUEST;
		ExpD exp = new ExpD(lambda);

		// Creating ID generator
		IDGenerator idGen = new IDGenerator();

		while (!freeSpace.isEmpty()) {
			int space = freeSpace.get(0);
			clientID = idGen.createID();

			while (space < NB_REQUEST) {
				IDs[space] = clientID;
				freeSpace.remove((Integer) space);
				space += (int) exp.sample();
			}
		}

		/*
		 * Creation of Requests
		 */
		List<Request.Builder> buildersRequests = Launcher.buildersRequests(termDist);
		List<Request> requests = new ArrayList<Request>();
		int i = 0;
		//request.time and request.applicationId are set here
		for (Request.Builder request : buildersRequests) {
			request.setTime(timeSequence.get(i)-startTime);
			request.setApplicationId(appLandscape.getApplicationsList().get(0).getApplicationId());

			requests.add(request.build());

			i++;
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

		System.out.println("Workload generated:" + (System.currentTimeMillis() - startTime));

		return workloadBuilder.build();
	}

	/**
	 * Method to generate a general ApplicationLandscape, based on
	 * LinknovateValidarionRAM.GenerateLinknovateValidationApplication (@author
	 * Malika)
	 */
	public static ApplicationLandscape GenerateApplicationLandscape(int appQty, int NB_SHARDS,
			Infrastructure infrastructure) {

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
			/*
			 * New application builder
			 */
			Application.Builder appBuilder = Application.newBuilder();
			appBuilder.setApplicationId("" + appCounter).setApplicationName("" + appCounter);

			/*
			 * Component for WS
			 */
			Component.Builder webServerBuilder = Component.newBuilder();
			webServerBuilder.setComponentName("Web server").setComponentId("1").setIsLoadbalanced(false);

			/*
			 * Deployment for WS
			 */
			Deployment.Builder deployment_webServer = Deployment.newBuilder();
			deployment_webServer.setNodeId(nodeIds.get(nodesCounter));
			// reset or advance counter
			nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;
			webServerBuilder.setDeployment(deployment_webServer.build());

			/*
			 * Api paths VM1
			 */
			// path 1
			String apiId = "1_1"; // Component ID, API ID
			Component.Api.Builder webServerApi_1 = Component.Api.newBuilder();
			webServerApi_1.setApiId(apiId);
			webServerApi_1.setApiName(webServerBuilder.getComponentName() + "_" + apiId);
			// resource consumption
			webServerApi_1.setMips(clientToWebServer_mips);
			webServerApi_1.setIops(clientToWebServer_iops);
			webServerApi_1.setRam(clientToWebServer_ram);
			webServerApi_1.setDataToTransfer(clientToWebServer_transferData);
			// connect to next api
			webServerApi_1.addNextComponentId("2");
			webServerApi_1.addNextApiId("2_1");
			webServerBuilder.addApis(webServerApi_1.build());

			// path 2
			apiId = "1_2"; // Component ID, API ID
			Component.Api.Builder webServerApi_7 = Component.Api.newBuilder();
			webServerApi_7.setApiId(apiId);
			webServerApi_7.setApiName(webServerBuilder.getComponentName() + "_" + apiId);
			// resource consumption
			webServerApi_7.setMips(ESToWebServer_mips);
			webServerApi_7.setIops(ESToWebServer_iops);
			webServerApi_7.setRam(ESToWebServer_ram);
			webServerApi_7.setDataToTransfer(ESToWebServer_transferData);
			// connect to next api
			// no add of next component
			// no add of next api
			webServerBuilder.addApis(webServerApi_7.build());

			// create and add flavour
			VeFlavour.Builder veFlavour_controlPlane = VeFlavour.newBuilder();
			veFlavour_controlPlane.setCores(vmCores);
			veFlavour_controlPlane.setMemory(vmMemory);
			veFlavour_controlPlane.setStorage(vmStorage);
			webServerBuilder.setFlavour(veFlavour_controlPlane.build());
			appBuilder.addComponents(webServerBuilder.build());

			/*
			 * Component for ES client
			 */
			Component.Builder esClientBuilder = Component.newBuilder();
			esClientBuilder.setComponentName("ES Client").setComponentId("2").setIsLoadbalanced(false);

			/*
			 * Deployment for ES client
			 */
			Deployment.Builder deployment_esClient = Deployment.newBuilder();
			deployment_esClient.setNodeId(nodeIds.get(nodesCounter));
			// reset or advance counter
			nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;
			esClientBuilder.setDeployment(deployment_esClient.build());

			/*
			 * Api paths VM2
			 */
			// path 1
			apiId = "2_1"; // Component ID, API ID
			Component.Api.Builder esClientApi_1 = Component.Api.newBuilder();
			esClientApi_1.setApiId(apiId);
			esClientApi_1.setApiName(esClientBuilder.getComponentName() + "_" + apiId);
			// resource consumption
			esClientApi_1.setMips(webServerToES_mips);
			esClientApi_1.setIops(webServerToES_iops);
			esClientApi_1.setRam(webServerToES_ram);
			esClientApi_1.setDataToTransfer(webServerToES_transferData);
			// connect to next api
			esClientApi_1.addNextComponentId("3");
			esClientApi_1.addNextApiId("3_1");
			esClientApi_1.addNextComponentId("4");
			esClientApi_1.addNextApiId("4_1");
			esClientApi_1.addNextComponentId("5");
			esClientApi_1.addNextApiId("5_1");
			esClientApi_1.addNextComponentId("6");
			esClientApi_1.addNextApiId("6_1");
			esClientApi_1.addNextComponentId("7");
			esClientApi_1.addNextApiId("7_1");
			esClientApi_1.addNextComponentId("8");
			esClientApi_1.addNextApiId("8_1");

			esClientBuilder.addApis(esClientApi_1.build());

			// path 2
			apiId = "2_2"; // Component ID, API ID
			Component.Api.Builder esClientApi_7 = Component.Api.newBuilder();
			esClientApi_7.setApiId(apiId);
			esClientApi_7.setApiName(esClientBuilder.getComponentName() + "_" + apiId);
			// resource consumption
			esClientApi_7.setMips(DataNodeToES_mips);
			esClientApi_7.setIops(DataNodeToES_iops);
			esClientApi_7.setRam(DataNodeToES_ram);
			esClientApi_7.setDataToTransfer(DataNodeToES_transferData);
			// connect to next api
			esClientApi_7.addNextComponentId("1");
			esClientApi_7.addNextApiId("1_2");
			esClientBuilder.addApis(esClientApi_7.build());

			// create flavour
			VeFlavour.Builder veFlavour_esClient = VeFlavour.newBuilder();
			veFlavour_esClient.setCores(16);
			veFlavour_esClient.setMemory(112_000);
			veFlavour_esClient.setStorage(181_000);

			esClientBuilder.setFlavour(veFlavour_esClient.build());
			appBuilder.addComponents(esClientBuilder.build());

			/*
			 * Creating, deploying and building shards
			 */
			for (int shard = 1; shard <= NB_SHARDS; shard++) {

				String componentId = Integer.toString(nodesCounter);
				Component.Builder shardBuilder = createShardComponent("Shard_" + shard, Integer.toString(2 + shard),
						componentId + "_1", nodeIds.get(nodesCounter));

				nodesCounter = (nodesCounter == indexNmberOfNodes) ? 0 : nodesCounter + 1;

				appBuilder.addComponents(shardBuilder.build());
			}

			applicationLandscapeBuilder.addApplications(appBuilder.build());

		}

		System.out.println("ApplicationLandscape generated:" + (System.currentTimeMillis() - startTime));

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
		for (int i = 0; i < numberSites; i++) {

			ResourceSite.Builder site = ResourceSite.newBuilder();
			site.setName("Site_" + i);
			site.setId(i + "");

			Location.Builder geolocation = Location.newBuilder();
			geolocation.setLatitude(i);
			geolocation.setLongitude(i);

			site.setLocation(geolocation.build());
			site.setHierarchyLevel(SiteLevel.Edge);

			// create nodes
			for (int j = 0; j < numberNodesPerSites[i]; j++) {

				Node.Builder node = Node.newBuilder();
				node.setName("Node_" + i + "_" + j);
				node.setId(i + "_" + j);

				// TODO parameters cpu builder
				CPU.Builder cpu = CPU.newBuilder();
				cpu.setName("Xeon_" + i + "_" + j);
				cpu.setId(i + "_" + j);
				cpu.setMake("Intel");
				cpu.setRating("12345");
				cpu.setFrequency(cpuFrequency[i][j]);
				// create cores
				for (int e = 0; e < cpuCores[i][j]; e++) {
					Core.Builder core = Core.newBuilder();
					core.setId(i + "_" + j + "_" + e);
					cpu.addCpuCores(core.build());
				}

				Memory.Builder memory = Memory.newBuilder();
				memory.setId(i + "_" + j);
				memory.setCapacity(ram[i][j]);

				Storage.Builder storage = Storage.newBuilder();
				storage.setId(i + "_" + j);
				storage.setSize(hdd[i][j]);

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

		System.out.println("Infrastructure generated:" + (System.currentTimeMillis() - startTime));

		return infrastructure.build();

	}

	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////// METHODS
	////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Generates an array containing the same value everywhere, for testing
	 */
	public static int[][] initSameValue(int nbSites, int[] nbNodesPerSite, int value) {
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

	public static Component.Builder createShardComponent(String componentName, String componentId,
			String apiId /* Component ID, API ID */, String nodeId) {
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
		shardApi_1.setApiId(apiId);
		shardApi_1.setApiName(shard.getComponentName() + "_" + apiId);
		// resource consumption
		shardApi_1.setMips(ESToDataNode_mips);
		shardApi_1.setIops(ESToDataNode_iops);
		shardApi_1.setRam(ESToDataNode_ram);
		shardApi_1.setDataToTransfer(ESToDataNode_transferData);
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

class IDGenerator {
	private long id;

	public IDGenerator() {
		this.id = 0;
	}

	public String createID() {
		this.id++;
		return Long.toString(this.id);
	}
}