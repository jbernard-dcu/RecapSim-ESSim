package main;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.distribution.EnumeratedDistribution;

public class Test_cache {

	public static void main(String[] args) {

		EnumeratedDistribution<Long> dist = new EnumeratedDistribution<Long>(Launcher.ZipfDist(100, 1 / 3.));

		List<ShardSim> shardBase = new ArrayList<ShardSim>();
		for (int shard = 0; shard < 9; shard++) {
			shardBase.add(new ShardSim());
		}

		EnumeratedDistribution<Long> shardDist = new EnumeratedDistribution<Long>(Launcher.UnifDist(shardBase.size()));

		int c = 0;
		int nc = 0;

		for (int i = 0; i < 1_000; i++) {
			List<ShardSim> shardsR = new ArrayList<ShardSim>();
			int nbShards = 3/* Launcher.randGint(shardBase.size() / 2., shardBase.size() / 6.) */;
			for (int v = 0; v < nbShards; v++) {
				shardsR.add(shardBase.get(shardDist.sample().intValue()));
			}

			Long request = dist.sample();
			for (ShardSim s : shardsR) {
				int fr = s.fetchResults(request.intValue());
				if (fr < 0) {
					c += 1;
				} else {
					nc += 1;
				}
				System.out.println("Request:" + request + ", Shard" + shardsR.indexOf(s) + ", " + fr);
			}

		}

		System.out.println((double) c / (c + nc));

	}

}

class ShardSim {
	public List<Integer> cache;
	public int sizeCache;

	public ShardSim() {
		this.cache = new ArrayList<Integer>();
		this.sizeCache = 10;
	}

	public int fetchResults(Integer request) {
		if (this.cache.contains(request)) {
			updateCache(request);
			return -1;
		} else {
			updateCache(request);
			return 1;
		}
	}

	public void updateCache(Integer request) {

		if (this.cache.contains(request))
			this.cache.remove((Integer) request);

		if (this.cache.size() == this.sizeCache)
			this.cache.remove(0);

		this.cache.add(request);

	}
}
