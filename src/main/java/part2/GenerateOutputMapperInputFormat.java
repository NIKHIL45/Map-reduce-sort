package part2;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class GenerateOutputMapperInputFormat extends FileInputFormat<LongWritable, PrefixRankCustomWritable> {

	@Override
	public RecordReader<LongWritable, PrefixRankCustomWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new GenerateOutputMapperRecordReader();
	}

}
