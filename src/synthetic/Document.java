package synthetic;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import eu.recap.sim.models.WorkloadModel.Request;
import main.Launcher;

public class Document {

	private List<Long> content;
	private long sizeBytes;
	private int docID;

	/**
	 * TODO
	 * 
	 * <link>
	 * https://www.elastic.co/guide/en/elasticsearch/reference/current/documents-indices.html</link>
	 * Add full document structure
	 * 
	 */
	public Document(List<Long> content, long sizeBytes, int docID) {
		this.content = content;
		this.sizeBytes = sizeBytes;
		this.docID = docID;
	}

	public List<Long> getContent() {
		return this.content;
	}

	public long getSizeBytes() {
		return this.sizeBytes;
	}

	public int getID() {
		return this.docID;
	}

	public String toString() {
		return Integer.toString(this.docID);
	}
	
	@Override
	public boolean equals(Object o) {
		return o instanceof Document
				&& ((Document)o).getID()==this.getID();
	}
	

	/**
	 * Calculates the score of the document on responding the specified request,
	 * according to Lucene's Practical Scoring Function
	 * @throws Exception 
	 */
	public double getScoreRequest(Request request, List<Shard> shardbase, int sizeDatabase) throws Exception {
		// TODO : index boost, query boost, field normalization (fields in general
		// actually)
		// https://www.compose.com/articles/how-scoring-works-in-elasticsearch/

		List<Long> qTerms = Launcher.unparse(request.getSearchContent());

		// Calculation of the query normalization and coordination factors
		double queryNorm = 0;
		int coord = 0;
		double sum = 0;
		int tf=0;
		double idf=0.;
		int nDocsContainTerm;
		double score;
		
		for (long term : qTerms) {
			// normalization factor : square weight of the term, Zipf distribution
			queryNorm += Math.pow(1. / term, 2);
			
			// coordination factor : number of terms that appear in the document
			if (this.content.contains(term))
				coord += 1;

			// term frequency : number of times the terms appears in the document
			tf = Collections.frequency(this.content, term);

			// inverse document frequency :
			nDocsContainTerm = 0;HashMap<Long,List<Document>> ii;
			for(Shard shard:shardbase) {
				ii=shard.getInvertedIndex();
				if (ii.containsKey(term)) {
					nDocsContainTerm+=ii.get(term).size();
				}
			}
			
			idf = 1 + Math.log(sizeDatabase / (nDocsContainTerm + 1));

			// Adding to sum
			sum += tf + Math.pow(idf, 2);
		}
		
		score=queryNorm * coord * sum;
		
		if (Double.isInfinite(score)||Double.isNaN(score)) {
			throw new Exception("Invalid value for score calculation:"+score);
		}

		return score;

	}
	

}
