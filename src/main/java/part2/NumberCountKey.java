package part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class NumberCountKey implements WritableComparable<NumberCountKey>{

	private LongWritable number;
	private LongWritable count;
	
	public NumberCountKey(LongWritable number, LongWritable count) {
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

	public int compareTo(NumberCountKey o) {
		return this.number.compareTo(o.number);
	}

}
