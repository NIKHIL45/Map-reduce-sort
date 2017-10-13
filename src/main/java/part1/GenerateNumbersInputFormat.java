package part1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import common.Constants;

public class GenerateNumbersInputFormat implements InputFormat<LongWritable, NullWritable>{

	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		long noOfNumbersToGenerate = job.getLong(Constants.NO_OF_NUMBERS_TO_GENERATE, 0);
		long noOfNumbersToGenerateBySplit = noOfNumbersToGenerate / numSplits;
		
		InputSplit[] splits = new InputSplit[numSplits];
		
		for(int split = 0; split < numSplits - 1; split++) {
			splits[split] = new GenerateNumbersInputSplit(split * noOfNumbersToGenerateBySplit, 
														 noOfNumbersToGenerateBySplit);
		}
		splits[numSplits - 1] = new GenerateNumbersInputSplit((numSplits - 1) * noOfNumbersToGenerateBySplit, 
															noOfNumbersToGenerate - noOfNumbersToGenerateBySplit);
		
		return splits;
	}

	public RecordReader<LongWritable, NullWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new GenerateNumbersRecordReader();
	}
	

}
