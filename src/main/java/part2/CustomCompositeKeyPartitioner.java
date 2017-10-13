package part2;

import org.apache.hadoop.mapreduce.Partitioner;

public class CustomCompositeKeyPartitioner extends Partitioner<CustomCompositeKey, CustomKeyValueWritable> {

	@Override
	public int getPartition(CustomCompositeKey key, CustomKeyValueWritable value, int numPartitions) {
		return (int)key.getPartition().get();
	}

}
