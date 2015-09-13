package com.w2b.jexl;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.piggybank.storage.avro.AvroStorageUtils;

/**
 * Driver class for Jexl Demo.
 * 
 */
public class JexlDriver extends Configured implements Tool {

	public static HashMap<String, String> map = new HashMap<String, String>();
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new JexlDriver(), args);
	}

	public int run(String[] arg0) throws Exception {

		readArgs();
		System.out.println(map);
		
		getConf().set("inputpath", map.get("InputPath"));
		getConf().set("outputpath", map.get("OutputPath"));
		getConf().set("PrimaryKey", map.get("PrimaryKey"));
		
		Job job = new Job(getConf());
		job.setJarByClass(JexlDriver.class);
		
		Path inputPath = new Path(getConf().get("inputpath"));
		Path outputPath = new Path(getConf().get("outputpath"));
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputKeyClass(TextOutputFormat.class);
		
		AvroJob.setInputKeySchema(job, getSchema(inputPath));

		job.setMapperClass(JexlMapper.class);
		job.setNumReduceTasks(0);
		
		
		return job.waitForCompletion(true) ? 1 : 0;

	}

	private Schema getSchema(Path inputPath) throws IOException {
		FileSystem fs =  FileSystem.get(getConf());
		Path last = AvroStorageUtils.getLast(inputPath, fs);
		
		GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
		FSDataInputStream is = fs.open(last);
		DataFileStream<Object> ds = new DataFileStream<Object>(is,reader);
		Schema sc = ds.getSchema();
		ds.close();
		return sc;
	}
	
	private static void readArgs() throws Exception {

		Scanner scanner = new Scanner(new FileReader("jexl_properties.txt"));

		try {

			while (scanner.hasNextLine()) {
				String[] columns = scanner.nextLine().split(":");
				System.out.println("col: " + Arrays.toString(columns));
				map.put(columns[0], columns[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		} finally {
			scanner.close();
		}
	}
}
