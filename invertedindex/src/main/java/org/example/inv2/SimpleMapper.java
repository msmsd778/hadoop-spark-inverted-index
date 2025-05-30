package org.example.inv2;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private final Text word = new Text();
    private final MapWritable fileMap = new MapWritable();
    private String filename;

    @Override
    protected void setup(Context ctx) {
        filename = ((FileSplit) ctx.getInputSplit()).getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        for (String token : value.toString().toLowerCase().split("\\W+")) {
            if (token.isEmpty()) continue;
            word.set(token);
            fileMap.clear();
            fileMap.put(new Text(filename), new IntWritable(1));
            ctx.write(word, fileMap);
        }
    }
}
