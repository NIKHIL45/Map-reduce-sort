package com.osu.edu;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import common.Constants;
import part1.GenerateNumbersInputFormat;
import part1.GenerateNumbersMapper;
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
		
		/************************ MR0 ******************************/
		Configuration conf0 = new Configuration();
		
		long rangeOfNumbersToGenerate = Long.parseLong(args[0]);
		long noOfNumbersToGenerate = (long) (1.5 * rangeOfNumbersToGenerate);
		int numOfMappersToGenerate = Integer.parseInt(args[1]);
		
		conf0.setLong(Constants.NO_OF_NUMBERS_TO_GENERATE, noOfNumbersToGenerate);
		conf0.setLong(Constants.RANGE_OF_NUMBERS_TO_GENERATE, rangeOfNumbersToGenerate);
		conf0.setInt(Constants.NO_OF_MAPPERS_TO_GENERATE_DATA, numOfMappersToGenerate);
		
		Job job0 = Job.getInstance(conf0, "Generate Data");
		job0.setJarByClass(SortAndRankMain.class);
		
		Path outputPath0 = new Path(args[2]);
		
		job0.setNumReduceTasks(0);
		
		job0.setInputFormatClass(GenerateNumbersInputFormat.class);
		job0.setMapOutputKeyClass(LongWritable.class);
		job0.setMapOutputValueClass(NullWritable.class);
		job0.setMapperClass(GenerateNumbersMapper.class);
		FileOutputFormat.setOutputPath(job0, outputPath0);
		job0.waitForCompletion(true);
		/************************ MR0 ******************************/
		
		/************************ MR1 ******************************/
		Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Count Numbers");
        job1.setJarByClass(SortAndRankMain.class);
        
        Path inputPath1 = outputPath0;
        Path partitionOutputPath1 = new Path(Constants.PARTITION_OUTPUT_PATH);
        Path outputPath1 = new Path(Constants.MR1_OUTPUT_PATH);
        
        job1.setNumReduceTasks(Integer.parseInt(args[4]));
        FileInputFormat.setInputPaths(job1, inputPath1);
        TotalOrderPartitioner.setPartitionFile(job1.getConfiguration(), partitionOutputPath1);
        
        job1.setInputFormatClass(CountMapperInputFormat.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        
        InputSampler.Sampler<LongWritable, Text> sampler = 
        		new InputSampler.RandomSampler(0.01, 1000, 100);
        InputSampler.writePartitionFile(job1, sampler);
        
        job1.setPartitionerClass(TotalOrderPartitioner.class);
        job1.setMapperClass(CountMapper.class);
        job1.setCombinerClass(CountReducer.class);
        job1.setReducerClass(CountReducer.class);
        
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job1, outputPath1);
        job1.waitForCompletion(true);
        /************************ MR1 ******************************/
        
        /************************ MR2 ******************************/
        Configuration conf2 = new Configuration();
        conf2.setLong(Constants.RANGE_OF_NUMBERS_TO_GENERATE, rangeOfNumbersToGenerate);
        Job job2 = Job.getInstance(conf2, "Calculate Prefix Sums and Rank");
        job2.setJarByClass(SortAndRankMain.class);
        
        Path job2InputPath = outputPath1;
        FileInputFormat.setInputPaths(job2, job2InputPath);
        Path job2OutputPath = new Path(Constants.MR2_OUTPUT_PATH);
        
        job2.setNumReduceTasks(Integer.parseInt(args[5]));
        job2.setInputFormatClass(SplittingMapperInputFormat.class);
        job2.setMapOutputKeyClass(CustomCompositeKey.class);
        job2.setMapOutputValueClass(CustomKeyValueWritable.class);
        
        job2.setPartitionerClass(CustomCompositeKeyPartitioner.class);
        job2.setGroupingComparatorClass(CustomCompositeKeyGroupingComparator.class);
        
        job2.setMapperClass(SplittingMapper.class);
        job2.setReducerClass(PrefixRankReducer.class);
        
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(PrefixRankCustomWritable.class);
        MultipleOutputs.addNamedOutput(job2, Constants.PREFIXSUM_NAMED_OUTPUT, TextOutputFormat.class, LongWritable.class, PrefixRankCustomWritable.class);
        MultipleOutputs.addNamedOutput(job2, Constants.OFFSETS_NAMED_OUTPUT, TextOutputFormat.class, LongWritable.class, PrefixRankCustomWritable.class);
        FileOutputFormat.setOutputPath(job2, job2OutputPath);
        job2.waitForCompletion(true);
        /************************ MR2 ******************************/
        
        /************************ MR3 ******************************/
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Generate Outputs");
        job3.setJarByClass(SortAndRankMain.class);
        
        Path job3InputPath = job2OutputPath;
        FileInputFormat.setInputPaths(job3, Constants.PrefixSumAndRankOutputPath);
        Path job3OutputPath = new Path(args[3]);
        
        job3.setNumReduceTasks(0);
//        job3.addCacheFile(new URI("hdfs://58dde91ca0ae:9000"+Constants.OffsetsOutputPath));
        job3.addCacheFile(new URI(Constants.OffsetsOutputPath));

        job3.setInputFormatClass(GenerateOutputMapperInputFormat.class);
        job3.setMapperClass(GenerateOutputMapper.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(OutputWritable.class);
        
        FileOutputFormat.setOutputPath(job3, job3OutputPath);
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
        /************************ MR3 ******************************/
        
        
	}
}
