package part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class CustomSortValue implements Writable{

	private LongWritable rank;
	private LongWritable denseRank;
	private LongWritable startValue;
	private LongWritable noOfValuesInThisReducer;
	
	public void readFields(DataInput in) throws IOException {
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
