package org.example.invertedindex;
import java.util.Arrays;
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
            System.err.println("Usage: Driver <input> <numReducers> <output>");
            System.exit(1);
        }

        System.out.println("### Driver.class that is running was compiled on " 
            + Driver.class.getPackage().getImplementationVersion());
        System.out.println("### ARGS = " + Arrays.toString(args));
        System.out.println("### Input path: " + args[0]);
        System.out.println("### Num reducers: " + args[1]);
        System.out.println("### Output path: " + args[2]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(Driver.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setNumReduceTasks(Integer.parseInt(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.out.println("### Submitting job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
