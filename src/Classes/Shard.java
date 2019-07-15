package Classes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import Main.Launcher;
import eu.recap.sim.models.InfrastructureModel.Node;
import eu.recap.sim.models.WorkloadModel.Request;

public class Shard {

	private HashMap<Long, List<Document>> invertedIndex = new HashMap<Long, List<Document>>();
	private List<Shard> replicationGroup = new ArrayList<Shard>();
	private boolean isPrimaryShard = false;
	private Node node;
	private int shardId;

	/**
	 * Default constructor for the Shard object</br>
	 * invertedIndex and replicationGroup are empty Collections, primaryShard=false
	 */
	public Shard(Node node, int shardId) {
		this.node = node;
		this.shardId = shardId;
		addToReplicationGroup(this);
	}

	/**
	 * adds the document <code>doc</code> to the <code>invertedIndex</code>
	 */
	public void addDocument(Document doc) {
		if (this.isPrimaryShard()) {
			for (long word : doc.getContent()) {
				if (invertedIndex.containsKey(word) && !invertedIndex.get(word).contains(doc)) {
					invertedIndex.get(word).add(doc);
				} else {
					List<Document> neu = new ArrayList<Document>();
					neu.add(doc);
					invertedIndex.put(word, neu);
				}
			}
			for (Shard replica : this.replicationGroup) {
				if (!replica.equals(this)) {
					replica.setInvertedIndex(this.getInvertedIndex());
				}
			}
		} else {
			throw new IllegalArgumentException("ERROR : Can't add a document to a non-primary shard");
		}

	}

	/*
	 * TODO fix the 1 document results and finish Problem --> deal with infinity !!
	 */
	public TreeMap<Double, Document> fetchResults(Request request, List<Shard> shardbase, double p_Doc)
			throws Exception {
		List<Long> searchContent = unparse(request.getSearchContent());
		HashSet<Document> res = new HashSet<Document>();

		// Generating list of concerned documents
		for (long word : searchContent) {
			if (this.invertedIndex.containsKey(word)) {
				res.addAll(this.invertedIndex.get(word));
			}
		}

		// Calculating size of the shard level database
		int sldbSize = 0;
		for (long key : this.invertedIndex.keySet()) {
			sldbSize += this.invertedIndex.get(key).size();
		}

		// Sorting res according to score
		TreeMap<Double, Document> mapScores = new TreeMap<Double, Document>();
		for (Document doc : res) {
			mapScores.put(doc.getScoreRequest(request, shardbase, sldbSize), doc);
		}

		Collection<Document> rep = mapScores.values();

		System.out.println(rep.toString());
		System.out.println("sldb size:" + sldbSize);
		System.out.println("start:" + (int) ((1 - p_Doc) * rep.size()) + "; size/end:" + rep.size());

		// return new ArrayList<Document>(rep).subList((int)((1-p_Doc)*rep.size()),
		// rep.size());

		return mapScores;

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

	///////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////// OTHER METHODS, GETTERS, SETTERS
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	public String toString() {
		return Integer.toString(this.getId());
	}

	public HashMap<Long, List<Document>> getInvertedIndex() {
		return this.invertedIndex;
	}

	public void setInvertedIndex(HashMap<Long, List<Document>> ii) {
		this.invertedIndex = ii;
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	/**
	 * Returns the integer ID of the shard
	 */
	public int getId() {
		return this.shardId;
	}

	/**
	 * Returns the boolean stating if the shard is primary or not
	 */
	public boolean isPrimaryShard() {
		return this.isPrimaryShard;
	}

	/**
	 * If <code>primaryShard=true</code>, then <code>this</code> is new primary
	 * shard and all other shards from the replication group are set as
	 * replicas</br>
	 * Else, just set as not-primary shard (replica)
	 */
	public void setPrimaryShard(boolean primaryShard) {
		this.isPrimaryShard = primaryShard;
		if (primaryShard) {
			for (Shard shard : this.replicationGroup)
				if (!shard.equals(this))
					shard.setPrimaryShard(false);
		}
	}

	/**
	 * Returns the replication group of <code>this</code> shard
	 */
	public List<Shard> getReplicationGroup() {
		return this.replicationGroup;
	}

	/**
	 * Adds a shard to this shard's replication group</br>
	 * <code>this</code> is added by default in the replication group on call of the
	 * constructor
	 */
	public void addToReplicationGroup(Shard s) {
		this.replicationGroup.add(s);
	}

	/**
	 * Sets the replication group as the specified parameter
	 */
	public void setReplicationGroup(List<Shard> replicationGroup) {
		this.replicationGroup = replicationGroup;
	}

}
