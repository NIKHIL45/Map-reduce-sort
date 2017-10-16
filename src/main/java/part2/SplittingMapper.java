package part2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import common.Constants;

public class SplittingMapper extends Mapper<LongWritable, LongWritable, CustomCompositeKey, CustomKeyValueWritable> {

	private long divisor;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		int numberOfReducers = context.getNumReduceTasks();
		long rangeOfNumbers = conf.getLong(Constants.RANGE_OF_NUMBERS_TO_GENERATE, 0);
		
		divisor = (rangeOfNumbers / numberOfReducers) + 1;
		
	}

	@Override
	protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
		context.write(new CustomCompositeKey(new LongWritable(key.get() / this.divisor), key), 
					  new CustomKeyValueWritable(key, value));
	}
}
