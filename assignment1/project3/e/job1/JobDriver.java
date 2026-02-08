import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

import java.io.IOException;

public class JobDriver {
    public static class JobMapper_Activity extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable outK = new IntWritable(); // Create a IntWritable object for output key
        private IntWritable outV = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                outK.set(Integer.parseInt(records[1]));
                outV.set(Integer.parseInt(records[2]));
                context.write(outK, outV);
            }
        }
    }

    public static class JobMapper_NetPage extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable outK = new IntWritable(); // Create a IntWritable object for output key
        private IntWritable outV = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                outK.set(Integer.parseInt(records[0]));
                outV.set(-1);
                context.write(outK, outV);
            }
        }
    }

    public static class JobReducer_All extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        private Text outV = new Text();
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            List<Integer> visitList = new ArrayList<>();
            for (IntWritable value : values) {
                if (value.get() != -1) {
                    visitList.add(value.get());
                }
            }
            int total_visit = visitList.size();
            int distinct_visit = new HashSet<>(visitList).size();
            String out = Integer.toString(total_visit) + ", " + Integer.toString(distinct_visit);
            outV.set(out);
            // System.out.println("debug: " + key + ", " + user_follows + ", " + user_name);
            // write out the result (output (key, value) pair of reduce phase)
            context.write(new IntWritable(key.get()), outV);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. map the classes
        job.setJarByClass(JobDriver.class);
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/CircleNetPage.csv"), TextInputFormat.class, JobMapper_NetPage.class);
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/ActivityLog.csv"), TextInputFormat.class, JobMapper_Activity.class); // second mapper
        // job.setMapperClass(JobMapper.class);
        // job.setCombinerClass(Combiner.class);
        job.setReducerClass(JobReducer_All.class);

        // 3. set up the output key value data type class
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 4. Specify the input and output path
        // FileInputFormat.setInputPaths(job, new Path("/home/zhiyang/data/CircleNetPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/zhiyang/output/project3e/"));

        // job.setNumReduceTasks(1); // important to have a single reducer for top 10 calculation

        // 5. submit job!
        boolean result = job.waitForCompletion(true);
        
        System.exit(result ? 0 : 1);
    }
}