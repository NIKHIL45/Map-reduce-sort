package part1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class GenerateNumbersMapper extends Mapper<LongWritable, NullWritable, LongWritable, NullWritable> {

	@Override
	protected void map(LongWritable key, NullWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		context.write(key, value);
	}

}
