package org.example.inv2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private final Text word = new Text();
    private final MapWritable fileMap = new MapWritable();
    private Map<String,Integer> counts;
    private String filename;

    @Override
    protected void setup(Context ctx) {
        counts   = new HashMap<>();
        filename = ((FileSplit) ctx.getInputSplit()).getPath().getName();
	System.out.println("### MAP setup: filename = " + filename);
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) {
        System.out.println("### MAP input line: " + value.toString());

        String[] tokens = value.toString().split("\\s+");
        for (String token : tokens) {
            System.out.println("### MAP token: " + token);
        }


        for (String token : value.toString().toLowerCase().split("\\W+")) {
            if (!token.isEmpty())
                counts.merge(token, 1, Integer::sum);
        }
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        for (Map.Entry<String,Integer> e : counts.entrySet()) {
            word.set(e.getKey());
            fileMap.clear();
            fileMap.put(new Text(filename), new IntWritable(e.getValue()));
            ctx.write(word, fileMap);
        }
    }
}
