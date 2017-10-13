package com.osu.edu;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import common.Constants;
import part2.CountMapper;
import part2.CountMapperInputFormat;
import part2.CountReducer;
import part2.CustomCompositeKey;
import part2.CustomCompositeKeyGroupingComparator;
import part2.CustomCompositeKeyPartitioner;
import part2.CustomKeyValueWritable;
import part2.GenerateOutputMapper;
import part2.GenerateOutputMapperInputFormat;
import part2.OutputWritable;
import part2.PrefixRankCustomWritable;
import part2.PrefixRankReducer;
import part2.SplittingMapper;
import part2.SplittingMapperInputFormat;

public class SortAndRankMain {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
//		Configuration conf = new Configuration();
//        Job job1 = Job.getInstance(conf, "Count Numbers");
//        job1.setJarByClass(SortAndRankMain.class);
//        
//        Path inputPath = new Path(args[0]);
//        Path partitionOutputPath = new Path(args[1]);
//        Path outputPath = new Path(args[2]);
//        
//        job1.setNumReduceTasks(3); // TODO: parameterize this
//        FileInputFormat.setInputPaths(job1, inputPath);
//        TotalOrderPartitioner.setPartitionFile(job1.getConfiguration(), partitionOutputPath);
//        
//        job1.setInputFormatClass(CountMapperInputFormat.class);
//        job1.setMapOutputKeyClass(LongWritable.class);
//        job1.setMapOutputValueClass(LongWritable.class);
//        
//        InputSampler.Sampler<LongWritable, Text> sampler = 
//        		new InputSampler.RandomSampler(0.01, 1000, 100); // TODO: To be configured
//        InputSampler.writePartitionFile(job1, sampler);
//        
//        job1.setPartitionerClass(TotalOrderPartitioner.class);
//        job1.setMapperClass(CountMapper.class);
//        job1.setCombinerClass(CountReducer.class);
//        job1.setReducerClass(CountReducer.class);
//        
//        job1.setOutputKeyClass(LongWritable.class);
//        job1.setOutputValueClass(LongWritable.class);
//        FileOutputFormat.setOutputPath(job1, outputPath);
//        job1.waitForCompletion(true);
        
        /************************ MR2 ******************************/
//        Configuration conf2 = new Configuration();
//        Job job2 = Job.getInstance(conf2, "Calculate Prefix Sums and Rank");
//        job2.setJarByClass(SortAndRankMain.class);
//        
//        Path job2InputPath = outputPath;
//        FileInputFormat.setInputPaths(job2, job2InputPath);
        Path job2OutputPath = new Path(args[3]);
//        
//        job2.setNumReduceTasks(3); // TODO: parameterize this
//        job2.setInputFormatClass(SplittingMapperInputFormat.class);
//        job2.setMapOutputKeyClass(CustomCompositeKey.class);
//        job2.setMapOutputValueClass(CustomKeyValueWritable.class);
//        
//        job2.setPartitionerClass(CustomCompositeKeyPartitioner.class);
//        job2.setGroupingComparatorClass(CustomCompositeKeyGroupingComparator.class);
//        
//        job2.setMapperClass(SplittingMapper.class);
//        job2.setReducerClass(PrefixRankReducer.class);
//        
//        job2.setOutputKeyClass(LongWritable.class);
//        job2.setOutputValueClass(PrefixRankCustomWritable.class);
//        MultipleOutputs.addNamedOutput(job2, "prefixSumAndRank", TextOutputFormat.class, LongWritable.class, PrefixRankCustomWritable.class);
//        MultipleOutputs.addNamedOutput(job2, "offsets", TextOutputFormat.class, LongWritable.class, PrefixRankCustomWritable.class);
//        FileOutputFormat.setOutputPath(job2, job2OutputPath);
//        job2.waitForCompletion(true);
        /************************ MR2 ******************************/
        
        /************************ MR3 ******************************/
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Generate Outputs");
        job3.setJarByClass(SortAndRankMain.class);
        
        Path job3InputPath = job2OutputPath;
        FileInputFormat.setInputPaths(job3, Constants.PrefixSumAndRankOutputPath);
        Path job3OutputPath = new Path(args[4]);
        
        job3.setNumReduceTasks(0);
//        job3.addCacheFile(new URI(Constants.OffsetsOutputPath));
//        job3.addCacheFile(new File(Constants.OffsetsOutputPath).toURI());
//        job3.addCacheFile(new Path(Constants.OffsetsOutputPath).toUri());
        job3.addCacheFile(new URI("hdfs://58dde91ca0ae:9000"+Constants.OffsetsOutputPath));

        job3.setInputFormatClass(GenerateOutputMapperInputFormat.class);
        job3.setMapperClass(GenerateOutputMapper.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(OutputWritable.class);
        
        FileOutputFormat.setOutputPath(job3, job3OutputPath);
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
        /************************ MR3 ******************************/
        
        
	}
}
