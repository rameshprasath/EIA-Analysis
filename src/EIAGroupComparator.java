import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class EIAGroupComparator extends WritableComparator {

	protected EIAGroupComparator() {
		super(EIAKey.class, true);
	}

	@Override
	public int compare(WritableComparable ek1, WritableComparable ek2) {
		EIAKey key1 = (EIAKey) ek1;
		EIAKey key2 = (EIAKey) ek2;

		return new Integer(key1.year).compareTo(key2.year);
	}	

}
