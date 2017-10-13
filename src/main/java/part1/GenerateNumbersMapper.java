package part1;

import java.io.IOException;
import java.util.Random;
import common.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenerateNumbersMapper extends Mapper<Object, NullWritable, Text, LongWritable> {

	private Text empty = new Text();
	private long noOfNumbersToGenerate;
	private long noOfMappers;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		noOfMappers = Long.parseLong(conf.get("mapred.map.tasks"));
		noOfNumbersToGenerate = conf.getLong(Constants.NO_OF_NUMBERS_TO_GENERATE, 100);
	}
	
	@Override
	protected void map(Object key, NullWritable ignoredValue,
			Context context)
			throws java.io.IOException, InterruptedException {
		Random random = new Random();
		int mapperId = context.getTaskAttemptID().getTaskID().getId();
		
//		for (long i = start; i < end; i++) {
			context.write(empty, new LongWritable(random.nextLong()));	
//		}
	};

}
