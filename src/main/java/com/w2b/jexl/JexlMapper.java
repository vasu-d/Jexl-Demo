package com.w2b.jexl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JexlMapper extends Mapper<AvroKey<Record>, NullWritable, Text, Text> {
	
	StringBuffer sb = null;
	private static Map<String,String> map = new HashMap<String,String>();
	private JexlEngine jexl = null;
	private MapContext jc = null;
	String output = null;
	private Record record;
	
	@Override
	protected void setup(Mapper<AvroKey<Record>, NullWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		sb = new StringBuffer();
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		for(Path path : cacheFiles) {
			if (path.toString().contains("jexl_expr.properties")) {
				try{
					BufferedReader br = new BufferedReader(new FileReader(path.toString()));
					String line;
					while((line = br.readLine()) != null) {
						map.put(line.split("-")[0], line.split("-")[1]);
					}
					br.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}
		jexl = new JexlEngine();
		jc = new MapContext();
	}
	
	@Override
	protected void map(AvroKey<Record> key, NullWritable value,
			Mapper<AvroKey<Record>, NullWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		record = key.datum();
		for (String exp_id : map.keySet()) {
			jc.set("record",record);
			Expression e = jexl.createExpression(map.get(exp_id));
			
			output = e.evaluate(jc).toString() + " - " + record.get(context.getConfiguration().get("PrimaryKey"));
			context.write(new Text(exp_id), new Text(output));
		}
	}
	
	@Override
	protected void cleanup(Mapper<AvroKey<Record>, NullWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}

}
