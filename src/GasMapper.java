import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GasMapper extends Mapper<LongWritable, Text, EIAKey, EIAKey> {

	EIAKey OutputKey = new EIAKey();


	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String data = value.toString();
		String[] values;

		try
		{
			values = data.split(",");
			context.getCounter(counters.TOTAL_RECORDS).increment(1);

			/* Expected Input Format (Comma separated fields - State(2 Char), YearMonth(6 digits), Price (Float)
			 * YearMonth and Price will be validated since State code has been used as input by the data sourcing program
			 * */

			int year=0;
			int month=0;

			//Mapper expects atleast 3 fields in the input text

			if(values.length >=3)
			{
				if(values[1].length() > 4)
				{
					year = Integer.parseInt(values[1].substring(0, 4));
					month = Integer.parseInt(values[1].substring(4));											
				}
				else
				{
					//Invalid period format
					context.getCounter(counters.BAD_RECORDS).increment(1);
				}

				double price = Double.parseDouble(values[2]);

				if ((month > 0 && month <= 12) && (year > 0))
				{
					//Populate Key information 
					OutputKey.stateCode = values[0];
					OutputKey.year = year;
					OutputKey.month = month;
					OutputKey.price = price;
				}			
				else
				{
					//Invalid month in the period parameter
					context.getCounter(counters.BAD_RECORDS).increment(1);
				}			

			}
			else
			{
				//Less than 3 parameters in input record
				context.getCounter(counters.BAD_RECORDS).increment(1);
			}

			if (OutputKey.stateCode != ""  && OutputKey.year > 0 && OutputKey.month > 0 && OutputKey.price > 0)
				context.write(OutputKey, OutputKey);
		}

		catch(NumberFormatException nfe)
		{
			context.getCounter(counters.BAD_RECORDS).increment(1);
			System.out.println("Invalid data found on numerical field - " + nfe.getMessage());
		}

		catch(Exception ex)
		{
			//Unable to process the input record
			context.getCounter(counters.ERROR_RECORDS).increment(1);
			System.out.println(ex.getMessage());
		}

	}

}

