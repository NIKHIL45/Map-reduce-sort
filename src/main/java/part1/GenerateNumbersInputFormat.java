package part1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import common.Constants;

public class GenerateNumbersInputFormat extends InputFormat<LongWritable, NullWritable>{

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		int numSplits = conf.getInt(Constants.NO_OF_MAPPERS_TO_GENERATE_DATA, 0);
		
		List<InputSplit> splits = new ArrayList<>();
		
		for(int split = 0; split < numSplits; split++) {
			splits.add(new FakeInputSplit());
		}
		
		return splits;
	}

	@Override
	public RecordReader<LongWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		GenerateNumbersRecordReader recordReader = new GenerateNumbersRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}
	
	

}
