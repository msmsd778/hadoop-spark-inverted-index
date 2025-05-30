package org.example.inv2;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {

        MapWritable combined = new MapWritable();

        for (MapWritable value : values) {
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                Text file = (Text) entry.getKey();
                IntWritable count = (IntWritable) entry.getValue();

                if (combined.containsKey(file)) {
                    int sum = ((IntWritable) combined.get(file)).get() + count.get();
                    combined.put(file, new IntWritable(sum));
                } else {
                    combined.put(file, new IntWritable(count.get()));
                }
            }
        }

        context.write(key, combined);
    }
}
