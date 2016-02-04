
import org.apache.hadoop.mapreduce.Partitioner;


public class EIAGasPartitioner extends Partitioner<EIAKey, EIAKey>{

	@Override
	public int getPartition(EIAKey key, EIAKey value, int numPartitions) {
		int hash = 7;
        
		hash = 13 * hash + key.stateCode.hashCode();
		hash = 13 * hash + new Integer(key.year).hashCode();
        
		//hash = 13 * hash + (key.getState() != null ? key.getState().hashCode() : 0); 
        //hash = 13 * hash + (key.getPeriod() != null ? key.getPeriod().toString().substring(0, 4).hashCode() : 0);
        
        return hash % numPartitions;
	}

}
