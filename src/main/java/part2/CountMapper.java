package part2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

	private LongWritable one = new LongWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, one);
	}

}
