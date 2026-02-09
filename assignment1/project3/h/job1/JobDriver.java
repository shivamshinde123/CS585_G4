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
import java.util.*;

import java.io.IOException;

public class JobDriver {
    public static class JobMapper_NetPage extends Mapper<LongWritable, Text, IntWritable, Text> {

        private IntWritable outK = new IntWritable(); // Create a IntWritable object for output key
        private Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                outK.set(Integer.parseInt(records[0]));
                String outStr = records[1] + "," + records[3];
                outV.set(outStr);
                context.write(outK, outV);
            }
        }
    }

    public static class JobMapper_Follow extends Mapper<LongWritable, Text, IntWritable, Text> {

        private IntWritable outK = new IntWritable(); // Create a IntWritable object for output key
        private Text outV = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                outK.set(Integer.parseInt(records[1]));
                outV.set(records[2]);
                context.write(outK, outV);
            }
        }
    }

    public static class JobReducer_All extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text outV = new Text();
        Map<Integer, Integer> user_region = new HashMap<>();
        Map<Integer, Text> user_name = new HashMap<>();
        Map<Integer, List<Integer>> follower = new HashMap<>();
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            int user = key.get();
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length == 2) {
                    user_name.put(user, new Text(parts[0]));
                    user_region.put(user, Integer.parseInt(parts[1]));
                } else {
                    follower.computeIfAbsent(user, k -> new ArrayList<>()).add(Integer.parseInt(parts[0]));
                }
            }
            // System.out.println("debug: " + key + ", " + user_visit + ", " + user_info);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // System.out.println("top_visit.length: " + top_visit[0] + ", " + top_visit[1]);
            // System.out.println("top_visit.length: " + top_user[0]);
            for (Map.Entry<Integer, Integer> record : user_region.entrySet()) {
                Integer user = record.getKey();
                Integer region = record.getValue();
                List<Integer> followers = follower.get(user);
                if (followers != null) {
                    for (Integer followerId : followers) {
                        // use followerId
                        if (region.equals(user_region.get(followerId)) && !follower.get(followerId).contains(user)) {
                            context.write(new IntWritable(user), user_name.get(user));
                            break;
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. map the classes
        job.setJarByClass(JobDriver.class);
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/CircleNetPage.csv"), TextInputFormat.class, JobMapper_NetPage.class);
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/Follows.csv"), TextInputFormat.class, JobMapper_Follow.class); // second mapper
        // job.setMapperClass(JobMapper.class);
        // job.setCombinerClass(Combiner.class);
        job.setReducerClass(JobReducer_All.class);

        // 3. set up the output key value data type class
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 4. Specify the input and output path
        // FileInputFormat.setInputPaths(job, new Path("/home/zhiyang/data/CircleNetPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/zhiyang/output/project3h/"));

        job.setNumReduceTasks(1); // important to have a single reducer for top 10 calculation

        // 5. submit job!
        boolean result = job.waitForCompletion(true);
        
        System.exit(result ? 0 : 1);
    }
}