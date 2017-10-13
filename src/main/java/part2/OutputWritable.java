package part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import common.Constants;

public class OutputWritable implements Writable {

	private LongWritable lineNumber;
	private LongWritable rank;
	private LongWritable denseRank;

	public OutputWritable() {
		this.lineNumber = new LongWritable(0);
		this.rank = new LongWritable(0);
		this.denseRank = new LongWritable(0);
	}

	public OutputWritable(LongWritable lineNumber, LongWritable rank, LongWritable denseRank) {
		this.lineNumber = lineNumber;
		this.rank = rank;
		this.denseRank = denseRank;
	}

	public void readFields(DataInput in) throws IOException {
		this.lineNumber.readFields(in);
		this.rank.readFields(in);
		this.denseRank.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.lineNumber.write(out);
		this.rank.write(out);
		this.denseRank.write(out);
	}

	public LongWritable getLineNumber() {
		return lineNumber;
	}

	public LongWritable getRank() {
		return rank;
	}

	public LongWritable getDenseRank() {
		return denseRank;
	}

	@Override
	public String toString() {
		return this.lineNumber.toString() + Constants.SPACE + this.rank.toString() + Constants.SPACE
				+ this.denseRank.toString();
	}

}
