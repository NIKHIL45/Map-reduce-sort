package part2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SplittingMapper extends Mapper<LongWritable, LongWritable, CustomCompositeKey, CustomKeyValueWritable> {

//	private int numberOfReducers;
//	
//	@Override
//	protected void setup(Context context) throws IOException, InterruptedException {
//		this.numberOfReducers = context.getNumReduceTasks();
//	}

	@Override
	protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
		context.write(new CustomCompositeKey(new LongWritable(key.get() / 10000001), key), 
					  new CustomKeyValueWritable(key, value));
	}
}
