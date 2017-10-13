package part1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

public class GenerateNumbersRecordReader implements RecordReader<LongWritable, NullWritable>{

	public boolean next(LongWritable key, NullWritable value) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	public LongWritable createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	public NullWritable createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	

}
