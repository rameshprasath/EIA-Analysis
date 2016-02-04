import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class EIASortComparator extends WritableComparator{
	
	protected EIASortComparator() {
		super(EIAKey.class, true);		
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		EIAKey key1 = (EIAKey) a;
		EIAKey key2 = (EIAKey) b;
		
		int retVal = new Integer(key1.year).compareTo(key2.year);
		
		//For sorting DESC 
		if (retVal == 0)
			retVal = -1 * key1.price.compareTo(key2.price);

		return retVal;		
	}
}
