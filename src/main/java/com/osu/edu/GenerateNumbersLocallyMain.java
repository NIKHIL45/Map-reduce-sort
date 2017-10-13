package com.osu.edu;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class GenerateNumbersLocallyMain {
	public static void main(String[] args) throws IOException {
		long noOfNumbersToGenerate = (long) (1.5 * Double.parseDouble(args[0]));
		String outputFilePath = args[1];
		
		File outputFile = new File(outputFilePath);
		
		FileWriter fw = new FileWriter(outputFile, false);
		
		PrintWriter out = new PrintWriter(fw);
		
		Random random = new Random();
		
		for(long i = 0; i < noOfNumbersToGenerate; i++) {
			String nextRandomNumber = String.valueOf(Math.abs(random.nextLong() % noOfNumbersToGenerate + 1)); 
			out.println(nextRandomNumber);
		}
		fw.close();
		
	}
}
