package part1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class GenerateNumbersInputSplit implements InputSplit{
	
	private long startNo;
	private long noOfNumbersToGenerateBySplit;

	public GenerateNumbersInputSplit(long startNo, long noOfNumbersToGenerateBySplit) {
		this.startNo = startNo;
		this.noOfNumbersToGenerateBySplit = noOfNumbersToGenerateBySplit;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public long getLength() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	public String[] getLocations() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
