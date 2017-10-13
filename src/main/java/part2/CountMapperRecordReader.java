package part2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class CountMapperRecordReader extends RecordReader<LongWritable, Text> {

	private LineReader in;
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private int maxLineLength;
	private long start;
    private long pos;
    private long end;

	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		
		Configuration conf = context.getConfiguration();
		this.maxLineLength = conf.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
		
		start = split.getStart();
        end = start + split.getLength();
        
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(conf);
        
        FSDataInputStream fileIn = fs.open(split.getPath());
		
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            // Set the file pointer at "start - 1" position.
            // This is to make sure we won't miss any line
            // It could happen if "start" is located on a EOL
            --start;
            fileIn.seek(start);
        }
        
        in = new LineReader(fileIn, conf);
        
        // If first line needs to be skipped, read first line
        // and stores its content to a dummy Text
        if (skipFirstLine) {
            Text dummy = new Text();
            // Reset "start" to "start + line offset"
			start += in.readLine(dummy, 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
			
        }
        // Position is the actual start
        this.pos = start;
				
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
        int newSize = 0;
 
        // Make sure we get at least one record that starts in this Split
        while (pos < end) {
        		Text t = new Text();
            // Read first line and store its content to "value"
			newSize = in.readLine(t, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
 
            // No byte read, seems that we reached end of Split
            // Break and return false (no key / value)
            if (newSize == 0) {
                break;
            }
 
            // Line is read, new position is set
            pos += newSize;
            
            // Set key and value to be read
            String s = new String(t.toString());
//            System.out.println(s);
            key.set(Long.parseLong(s));
            value.set("");
            
 
            // Line is lower than Maximum record line size
            // break and return true (found key / value)
            if (newSize < maxLineLength) {
                break;
            }
 
        }
 
         
        if (newSize == 0) {
            // We've reached end of Split
            key = null;
            value = null;
            return false;
        } else {
            // Tell Hadoop a new line has been found
            // key / value will be retrieved by
            // getCurrentKey getCurrentValue methods
            return true;
        }
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
            in.close();
        }
	}
}
