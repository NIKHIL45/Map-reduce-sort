package part1;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import common.Constants;

public class GenerateNumbersRecordReader extends RecordReader<LongWritable, NullWritable>{

	
	private int createdRecords = 0;
	private LongWritable key = new LongWritable();
	private NullWritable value = NullWritable.get();
	private Random random = new Random();
	private long noOfNumbersToGenerate;
	private int numSplits;
	private long noOfNumbersToGenerateBySplit;
	private long rangeOfNumbersToGenerate;
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		this.noOfNumbersToGenerate = conf.getLong(Constants.NO_OF_NUMBERS_TO_GENERATE, 0);
		this.numSplits = conf.getInt(Constants.NO_OF_MAPPERS_TO_GENERATE_DATA, 5);
		this.noOfNumbersToGenerateBySplit = noOfNumbersToGenerate / numSplits;
		this.rangeOfNumbersToGenerate = conf.getLong(Constants.RANGE_OF_NUMBERS_TO_GENERATE, 20000000);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(createdRecords < noOfNumbersToGenerateBySplit) {
			key.set((Math.abs(random.nextLong() % rangeOfNumbersToGenerate + 1)));
			this.createdRecords++;
			return true;
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) createdRecords / (float) noOfNumbersToGenerateBySplit;
	}

	@Override
	public void close() throws IOException {
		
	}
	

}
