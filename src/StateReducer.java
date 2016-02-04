import java.io.IOException;
import java.text.DateFormatSymbols;
import java.util.ArrayList;
import java.util.List;
//import java.text.DateFormatSymbols;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StateReducer extends Reducer<EIAKey, EIAKey, EIAKey, Text> {

	Text OutputValue = new Text();

	@Override
	public void reduce(EIAKey key, Iterable<EIAKey> values, Context context)
			throws IOException, InterruptedException {

		double minPrice = Double.MAX_VALUE;
		double maxPrice = Double.MIN_VALUE;
		List<Integer> mxPMList = new ArrayList<Integer>();
		List<Integer> mnPMList = new ArrayList<Integer>();
		
		OutputValue.set("");
		String output = "";

		try
		{
			int cnt = 0;
			EIAKey minGas = null;			
						
			//First object contains Max gas price record and last record contains min gas price record
			for (EIAKey value : values) 
			{				
				cnt++;
				minGas = value;
				
				if (value.price > maxPrice)
				{
					maxPrice = value.price;
					mxPMList.clear();
					mxPMList.add(value.month);
				}
				else if (value.price == maxPrice)
				{
					mxPMList.add(value.month);
				}
				
				if (value.price < minPrice)
				{
					minPrice = value.price;
					mnPMList.clear();
					mnPMList.add(value.month);
				}
				else if (value.price == minPrice)
				{
					mnPMList.add(value.month);
				}
				
//				if (cnt == 1)
//				{
//					maxPrice = value.price;
//					maxPriceMonth = value.month;
//				}
				
			}	
			
			if (mxPMList.size() > 0 && mnPMList.size() > 0)
			{
				output = "Minimum gas price is " + minPrice;
				output += " on the month(s) ";
				String months = "";
				for (int i : mnPMList)
				{
					if (months != "") 
						months += ", ";
					months += new DateFormatSymbols().getMonths()[i-1];
				}
				
				output += months;
				output += " and Maximum gas price is " + maxPrice;
				output += " on the month(s) ";
				months = "";
				for (int i : mxPMList)
				{
					if (months != "") 
						months += ", ";
					months += new DateFormatSymbols().getMonths()[i-1];
				}		
				output += months;
				
				OutputValue.set(output);
				
				context.write(key,OutputValue);
			}
			
//			if (minGas != null)
//			{
//				minPrice = minGas.price;
//				//minPriceMonth = minGas.month;
//				
////				OutputValue.set(minGas.stateCode + " - For the year " + minGas.year + ", Minimum gas price is " + minPrice + " on the month " 
////						+  new DateFormatSymbols().getMonths()[minPriceMonth-1] + ", and maximum gas price is " + maxPrice
////						+ " on the month " +  new DateFormatSymbols().getMonths()[maxPriceMonth-1]);
//				
//				context.write(key, OutputValue);
//			}			
				
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
