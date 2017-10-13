package part2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long sum = 0;
		
		for(LongWritable value : values) {
			sum += value.get();
		}

		context.write(key, new LongWritable(sum));
	}

}
