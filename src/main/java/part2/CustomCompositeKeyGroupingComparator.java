package part2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomCompositeKeyGroupingComparator extends WritableComparator {
	
	protected CustomCompositeKeyGroupingComparator() {
		super(CustomCompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CustomCompositeKey key1 = (CustomCompositeKey) w1;
		CustomCompositeKey key2 = (CustomCompositeKey) w2;
		return key1.getPartition().compareTo(key2.getPartition());
	}
}
