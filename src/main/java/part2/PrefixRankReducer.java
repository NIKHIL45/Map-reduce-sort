package part2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import common.Constants;

public class PrefixRankReducer
		extends Reducer<CustomCompositeKey, CustomKeyValueWritable, LongWritable, PrefixRankCustomWritable> {

	private MultipleOutputs<LongWritable, PrefixRankCustomWritable> mos;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.mos = new MultipleOutputs<LongWritable, PrefixRankCustomWritable>(context);
	}
	
	@Override
	protected void reduce(CustomCompositeKey key, Iterable<CustomKeyValueWritable> values, Context context)
			throws IOException, InterruptedException {

		long prefixSum = 1;
		long denseRank = 1;

		for (CustomKeyValueWritable value : values) {
			mos.write("prefixSumAndRank", key.getPartition(), new PrefixRankCustomWritable(value.getNumber(),
					value.getCount(), new LongWritable(prefixSum), new LongWritable(denseRank)), Constants.PrefixSumAndRankOutputPath+"/prefixSumAndRank");
			prefixSum += value.getCount().get();	
			denseRank++;
		}
		mos.write("offsets", key.getPartition(), new PrefixRankCustomWritable(new LongWritable(1), new LongWritable(1),
													new LongWritable(prefixSum), new LongWritable(denseRank - 1)), Constants.OffsetsOutputPath+"/offsets");
	}
}
