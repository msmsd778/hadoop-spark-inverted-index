package org.example.inv2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, MapWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context ctx)
            throws IOException, InterruptedException {

       Map<String, Integer> aggregate = new HashMap<>();

       System.out.println("### Reducer received key: " + key.toString());

    for (MapWritable m : values) {
        for (Map.Entry<Writable, Writable> entry : m.entrySet()) {
            String file = entry.getKey().toString();
            int count = ((IntWritable) entry.getValue()).get();
            System.out.println("    -> file: " + file + ", count: " + count);

            aggregate.merge(file, count, Integer::sum);
        }
    }

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Integer> entry : aggregate.entrySet()) {
        sb.append(entry.getKey()).append(':').append(entry.getValue()).append('\t');
    }

    String output = sb.toString().trim();
    System.out.println("### Reducer output for key [" + key.toString() + "]: " + output);

    ctx.write(key, new Text(output));
}
}
