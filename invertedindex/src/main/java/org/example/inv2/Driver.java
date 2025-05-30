package org.example.inv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Driver <input> <numReducers> <output> [variant]");
            System.exit(1);
        }

        String input   = args[0];
        int reducers   = Integer.parseInt(args[1]);
        String output  = args[2];
        String variant = args.length > 3 ? args[3].toLowerCase() : "combiner";
	
	System.out.println(">>> I AM THE REAL DRIVER <<<");
	System.out.println("ARGS: " + java.util.Arrays.toString(args));

	
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(Driver.class);

        if ("imc".equals(variant)) {
            job.setMapperClass(InvertedIndexMapper.class);
        } else {
            job.setMapperClass(SimpleMapper.class);
        }
        if ("combiner".equals(variant)) {
            job.setCombinerClass(Combiner.class);
        }

        job.setReducerClass(InvertedIndexReducer.class);
        job.setNumReduceTasks(reducers);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
