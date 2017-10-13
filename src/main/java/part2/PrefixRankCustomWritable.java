package part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import common.Constants;

public class PrefixRankCustomWritable implements Writable {

	private LongWritable number;
	private LongWritable count;
	private LongWritable prefixSum;
	private LongWritable denseRank;

	public PrefixRankCustomWritable() {
		this.number = new LongWritable(0);
		this.count = new LongWritable(0);
		this.prefixSum = new LongWritable(0);
		this.denseRank = new LongWritable(0);
	}

	public PrefixRankCustomWritable(LongWritable number, LongWritable count, LongWritable prefixSum, LongWritable denseRank) {
		this.number = number;
		this.count = count;
		this.prefixSum = prefixSum;
		this.denseRank = denseRank;
	}

	public void readFields(DataInput in) throws IOException {
		this.number.readFields(in);
		this.count.readFields(in);
		this.prefixSum.readFields(in);
		this.denseRank.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.number.write(out);
		this.count.write(out);
		this.prefixSum.write(out);
		this.denseRank.write(out);
	}

	public LongWritable getNumber() {
		return number;
	}
	
	public LongWritable getCount() {
		return count;
	}

	public LongWritable getPrefixSum() {
		return prefixSum;
	}

	public LongWritable getDenseRank() {
		return denseRank;
	}

	public void setNumber(LongWritable number) {
		this.number = number;
	}

	public void setCount(LongWritable count) {
		this.count = count;
	}

	public void setPrefixSum(LongWritable prefixSum) {
		this.prefixSum = prefixSum;
	}

	public void setDenseRank(LongWritable denseRank) {
		this.denseRank = denseRank;
	}

	@Override
	public String toString() {
		return this.number.toString() + Constants.SPACE + this.count.toString() + Constants.SPACE
				+ this.prefixSum.toString() + Constants.SPACE + this.denseRank.toString();
	}

}
