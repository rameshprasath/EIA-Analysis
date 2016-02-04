import org.apache.hadoop.mapreduce.Partitioner;


public class EIAPartitioner extends Partitioner<EIAKey, EIAKey>{

	@Override
	public int getPartition(EIAKey key, EIAKey value, int numPartitions) {
		int hash = 7;
        
		hash = 13 * hash + new Integer(key.year).hashCode();

		return hash % numPartitions;
	}

}