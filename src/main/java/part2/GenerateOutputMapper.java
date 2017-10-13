package part2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class GenerateOutputMapper extends Mapper<LongWritable, PrefixRankCustomWritable, LongWritable, OutputWritable> {

	private BufferedReader br;
	private HashMap<Long, OffsetsVO> offsets = new HashMap<>();
	private OffsetsVO zeroOffset = new OffsetsVO(0,0,0);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		URI[] cacheFilesLocal = context.getCacheFiles();
		for (URI uri : cacheFilesLocal) {
				loadOffsetsIntoMemory(uri, context);
			}
	}
	
	@Override
	protected void map(LongWritable key, PrefixRankCustomWritable value, Context context)
			throws IOException, InterruptedException {
		OffsetsVO offset = offsets.get(key.get() - 1);
		if(offset == null) {
			offset = zeroOffset;
		}
		long lineNumberOffset = offset.getLineNumberOffset();
		long rankOffset = offset.getRankOffset();
		long denseRankOffset = offset.getDenseRankOffset();
		long lineNumber = value.getPrefixSum().get() + lineNumberOffset;
		long count = value.getCount().get();
		
		for(long i = 0; i < count; i++) {
			context.write(value.getNumber(),
					new OutputWritable(new LongWritable(lineNumber + i),
							new LongWritable(value.getPrefixSum().get() + rankOffset),
							new LongWritable(value.getDenseRank().get() + denseRankOffset)));
		}
	}
	
	private void loadOffsetsIntoMemory(URI uri, Context context) throws IOException {
		String strLineRead = "";
		Configuration conf = context.getConfiguration();
		try {
			Path path = new Path(uri);
	        FileSystem fs = path.getFileSystem(conf);
	        FileStatus[] status = fs.listStatus(path);
            for (int i=0;i<status.length;i++){
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
	    			while ((strLineRead = br.readLine()) != null) {
					String offsetsArray[] = strLineRead.split("\\s+");
					offsets.put(Long.parseLong(offsetsArray[0].trim()), new OffsetsVO(Long.parseLong(offsetsArray[3]),
							Long.parseLong(offsetsArray[3]), Long.parseLong(offsetsArray[4])));
				}
            }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				br.close();
			}
		}
		Set<Long> keys = offsets.keySet();
		List<Long> keyList = new ArrayList<>(keys);
		Collections.sort(keyList);
		for(Long key: keyList) {
			if(key > 0) {
				OffsetsVO o = offsets.get(key);
				OffsetsVO prev = offsets.get(key - 1);
				o.setLineNumberOffset(o.getLineNumberOffset() + prev.getLineNumberOffset());
				o.setRankOffset(o.getRankOffset() + prev.getRankOffset());
				o.setDenseRankOffset(o.getDenseRankOffset() + prev.getDenseRankOffset());
			}
		}
	}
}
