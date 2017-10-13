package part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import common.Constants;

public class CustomCompositeKey implements WritableComparable<CustomCompositeKey> {
	
	private LongWritable partition;
	private LongWritable number;
	
	public CustomCompositeKey() {
		this.partition = new LongWritable(0);
		this.number = new LongWritable(0);
	}
	
	public CustomCompositeKey(LongWritable partition, LongWritable number) {
		this.partition = partition;
		this.number = number;
	}

	public void readFields(DataInput in) throws IOException {
		this.partition.readFields(in);
		this.number.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.partition.write(out);
		this.number.write(out);		
	}

	public int compareTo(CustomCompositeKey o) {
		int partitionCompare = this.partition.compareTo(o.partition);
		if(partitionCompare != 0) return partitionCompare;
		return this.number.compareTo(o.number);
	}

	public LongWritable getPartition() {
		return partition;
	}

	public LongWritable getNumber() {
		return number;
	}
	
	@Override
	public String toString() {
		return this.partition.toString() + Constants.SPACE + this.number.toString();
	}

}
