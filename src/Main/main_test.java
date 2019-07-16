package Main;

import java.util.List;

import Classes.Generation;
import Classes.Print;
import Classes.Shard;
import eu.recap.sim.models.InfrastructureModel.Infrastructure;

public class main_test {
	
	public static void main(String[] args) {
		Infrastructure infrastructure = Generation.GenerateInfrastructure("");
		
		int NB_TOTALSHARDS = 16;
		int NB_REPLICAS = 3;
		
		List<Shard> shardBase = Generation.GenerateShardBase(infrastructure, NB_TOTALSHARDS, NB_REPLICAS);
		
		Print.printShardBase(shardBase);
	}

}
