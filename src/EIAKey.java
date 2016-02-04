import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class EIAKey implements WritableComparable<EIAKey>{
	public String stateCode;
	public int year;
	public int month;
	public Double price;

	public EIAKey(String stCode, int priceYear, int month, double price)
	{
		this.stateCode = stCode;
		this.year = priceYear;
		this.month = month;
		this.price = price;
	}

	public EIAKey()
	{
		this("",0,0,0.0);
	}



	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(stateCode);
		out.writeInt(year);
		out.writeInt(month);
		out.writeDouble(price);
	}



	@Override
	public void readFields(DataInput in) throws IOException {
		stateCode = in.readUTF();
		year = in.readInt();
		month = in.readInt();
		price = in.readDouble();
	}



	@Override
	public int compareTo(EIAKey OtherKey) {
		// Need to sort based on State, Year, Price. Ignore month for sorting
		int retVal = this.stateCode.compareTo(OtherKey.stateCode);

		if (retVal == 0)
		{
			retVal = new Integer(this.year).compareTo(OtherKey.year);
			
			if (retVal == 0)
				retVal = new Integer(this.month).compareTo(OtherKey.month);

			if (retVal == 0)
				retVal = this.price.compareTo(OtherKey.price);
		}		

		return retVal;
	}



	public String toString() {
		return stateCode + ", "
				+ year + ", "
				+ month + ", "
				+ price;
	}


	public boolean equals(Object o) {
		final EIAKey other = (EIAKey) o;

		return this.stateCode == other.stateCode &&
				this.year == other.year &&
				this.month == other.month &&
				this.price == other.price;
	}



	public int hashCode() {
		//^ new Integer(this.month).hashCode()
		return stateCode.hashCode()
				^ new Integer(this.year).hashCode();	
//				^ this.price.hashCode();
	}



	//Method used by Group comparator for key grouping. Ignore month and price for grouping the key
	public int compareGroup(EIAKey OtherKey) {
		int retVal = this.stateCode.compareTo(OtherKey.stateCode);

		if (retVal == 0)
			retVal = new Integer(this.year).compareTo(OtherKey.year);

		return retVal;
	}

}
