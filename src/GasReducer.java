import java.io.IOException;
import java.text.DateFormatSymbols;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GasReducer extends Reducer<EIAKey, EIAKey, Text, Text> {

	Text OutputValue = new Text();
	Text OutputKey = new Text();

	@Override
	public void reduce(EIAKey key, Iterable<EIAKey> values, Context context)
			throws IOException, InterruptedException {

		double maxPrice = Double.MIN_VALUE;
		String maxPriceState=""; 
		int maxMonth=-1;		
		
		try
		{
			int cnt = 0;
			EIAKey minGas = null;
			OutputKey.set(Integer.toString(key.year));
						
			//First object contains Max gas price record and last record contains min gas price record
			for (EIAKey value : values) 
			{				
				cnt++;
				minGas = value;
				
				if (cnt == 1)
				{
					maxPrice = value.price;
					maxMonth = value.month;
					maxPriceState = value.stateCode;
				}
			}
			
			if (minGas != null)
			{
				OutputValue.set("Minimum gas price is " + minGas.price + " on the month " 
						+  new DateFormatSymbols().getMonths()[minGas.month-1] + " in " + minGas.stateCode
						+ ", and maximum gas price is " + maxPrice
						+ " on the month " +  new DateFormatSymbols().getMonths()[maxMonth-1]
						+ " in " + maxPriceState);
				
				context.write(OutputKey, OutputValue);
			}	
		}
		catch(NumberFormatException NFex)
		{
			System.out.println(NFex.getMessage());
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}

}
