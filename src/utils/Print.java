package utils;

import java.util.List;
import java.util.TreeMap;

import org.apache.commons.math3.util.Pair;

import eu.recap.sim.models.WorkloadModel.Device;
import eu.recap.sim.models.WorkloadModel.Request;
import eu.recap.sim.models.WorkloadModel.Workload;
import main.Launcher;
import synthetic.Document;
import synthetic.Shard;

public class Print {
	
	public static void print(String value, long waitingTimeMillis) {
		System.out.println(value);
		try {
			Thread.sleep(waitingTimeMillis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static <T> void printMapMonitoring(TreeMap<Double,List<T>> map) {
		for(double key:map.keySet()) {
			System.out.printf("%8s %1s %30s", String.format("%.2f", key)," ",map.get(key).toString()+"\n");
		}
	}
	
	/**
	 * Prints the score of each document to the request q, using
	 * <code>getScoreRequest()</code> function
	 * @throws Exception 
	 */
	public static void printDocScore(List<Document> database, List<Shard> shardbase,  Request q) throws Exception {
		String printConfig = "%10s %1s %20s";
		System.out.printf(printConfig, "DocumentId", "|", "Score\n");
		for (Document doc : database) {
			System.out.printf(printConfig, doc.getID(), "|",
					doc.getScoreRequest(q,shardbase, database.size()) + "\n");
		}
	}

	/**
	 * Prints the specified shardBase with a column format</br>
	 * ShardId, Replication group, Primary shard, Node
	 */
	public static void printShardBase(List<Shard> shardBase) {
		String printConfig = "%7s %1s %17s %1s %13s %1s %4s";
		System.out.printf(printConfig, "ShardId", "|", "Replication group", "|", "Primary shard", "|", "Node\n");
		for (Shard shard : shardBase) {
			System.out.printf(printConfig, shard.getId(), "|", shard.getReplicationGroup().toString(), "|",
					shard.isPrimaryShard(), "|", shard.getNode().getId() + "\n");
		}
	}

	/**
	 * Prints the fetchResults of every Request of the workload</br>
	 * Request, Shard, Result
	 * @throws Exception 
	 */
	public static void printFetchResults(Workload workload, List<Shard> shardBase, double p_Doc) throws Exception {
		String printConfig = "%30s %1s %5s %1s %30s";
		System.out.printf(printConfig, "Request", "|", "Shard", "|", "Result\n");
		for (Device device : workload.getDevicesList()) {
			for (Request request : device.getRequestsList()) {
				for (Shard shard : shardBase) {
					System.out.printf(printConfig, request.getSearchContent(), "|", shard.getId(), "|",
							shard.fetchResults(request, shardBase, p_Doc).toString() + "\n");
				}
			}
		}
	}

	/**
	 * Prints the specified workload in a column format</br>
	 * Request, Time, Device, Score
	 */
	public static void printWorkload(Workload workload, List<Pair<Long, Double>> termDist, long startTime) {
		String configPrintf = "%32s %1s %8s %1s %6s %1s %17s";
		System.out.printf(configPrintf, "Request", "|", "Time", "|", "Device", "|", "Score\n");
		System.out.println("----------------------------------------------------------------------------------");
		for (Device device : workload.getDevicesList()) {
			for (Request request : device.getRequestsList()) {
				System.out.printf(configPrintf, request.getSearchContent().toString(), "|",
						(request.getTime() - startTime), "|", device.getDeviceId(), "|",
						Launcher.getWeight(request, termDist) + "\n");
			}
		}

		long endTime = System.currentTimeMillis();
		System.out.println("Start time:" + startTime + "     End time:" + endTime + "     Generating time:"
				+ (endTime - startTime) / 1000. + "s");
	}

}
