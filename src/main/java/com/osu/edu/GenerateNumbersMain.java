package com.osu.edu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import part1.GenerateNumbersMapper;

public class GenerateNumbersMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Generate Random Numbers");
        job.setNumReduceTasks(0);
        job.setMapperClass(GenerateNumbersMapper.class);
        job.setJarByClass(GenerateNumbersMain.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
