package part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class CustomKeyValueWritable implements Writable {
	
	private LongWritable number;
	private LongWritable count;

	public CustomKeyValueWritable() {
		this.number = new LongWritable(0);
		this.count = new LongWritable(0);
	}
	
	public CustomKeyValueWritable(LongWritable number, LongWritable count) {
		this.number = number;
		this.count = count;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.number.readFields(in);
		this.count.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.number.write(out);
		this.count.write(out);
	}

	public LongWritable getNumber() {
		return number;
	}

	public LongWritable getCount() {
		return count;
	}

}
