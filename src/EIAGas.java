import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;



public class EIAGas implements WritableComparable<EIAGas> {
	private Text state;
	private LongWritable period;
	private DoubleWritable price;


	public EIAGas()
	{
		
	}
	
	public EIAGas(Text SCode, LongWritable GasPeriod, DoubleWritable GasPrice)
	{
		setState(SCode);
		setPeriod(GasPeriod);
		setPrice(GasPrice);
	}

	public Text getState() {
		return state;
	}

	public void setState(Text state) {
		this.state = state;
	}

	public LongWritable getPeriod() {
		return period;
	}

	public void setPeriod(LongWritable period) {
		this.period = period;
	}

	public DoubleWritable getPrice() {
		return price;
	}

	public void setPrice(DoubleWritable price) {
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		state.write(out);
		period.write(out);
		price.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		state.readFields(in);
		period.readFields(in);
		price.readFields(in);
	}



	//This method used by GroupComparator for comparison

	public int compareGroup(EIAGas eiaGas) 
	{

		int retVal = getState().compareTo(eiaGas.getState());

		if (retVal == 0) {
			//Extract year and compare
			retVal = getPeriod().toString().substring(0, 4).compareTo(eiaGas.getPeriod().toString().substring(0, 4));
		}

		return retVal;
	}

	//Default hash partitioner would use this method to identify the Reducer

	@Override
	public int hashCode() 
	{
		int hash = 7;

		hash = 13 * hash + (this.state != null ? this.state.hashCode() : 0); 
		hash = 13 * hash + (this.period != null ? getPeriod().toString().substring(0, 4).hashCode() : 0);

		return hash;

	}


	//Default hash partitioner would use this method for comparison

	@Override
	public boolean equals(Object obj) 
	{
		final EIAGas other = (EIAGas) obj;

		return getState().equals(other.getState()) && 
				getPeriod().toString().substring(0, 4).equals(other.getPeriod().toString().substring(0, 4)); 


	}


	@Override
	public String toString()
	{
		return state + "\t" + period + "\t" + price;
	}


	@Override
	public int compareTo(EIAGas eiaGas) {
		int retVal = getState().compareTo(eiaGas.getState());

		//State matches, compare other fields
		if (retVal == 0)
		{
			//Extract year and compare
			retVal = getPeriod().toString().substring(0, 4).compareTo(eiaGas.getPeriod().toString().substring(0, 4));

			if (retVal == 0)
			{
				retVal = getPrice().compareTo(eiaGas.getPrice());				
			}
		}

		return retVal;		
	}

}


