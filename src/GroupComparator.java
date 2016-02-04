import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

	protected GroupComparator() {
		super(EIAKey.class, true);
	}
	
	@Override
    public int compare(WritableComparable ek1, WritableComparable ek2) {
		EIAKey key1 = (EIAKey) ek1;
		EIAKey key2 = (EIAKey) ek2;
		
		int retVal = key1.stateCode.compareTo(key2.stateCode);

		if (retVal == 0)
			retVal = new Integer(key1.year).compareTo(key2.year);

		return retVal;
    }	

}
