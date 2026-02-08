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

import java.io.IOException;

public class JobDriver {
    public static class JobMapper_Activity extends Mapper<LongWritable, Text, Text, Text> {

        private Text outK = new Text(); // Create a Text object for output key
        private Text outV = new Text("1");

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                outK.set(records[2]);
                context.write(outK, outV);
            }
        }
    }

    public static class JobMapper_NetPage extends Mapper<LongWritable, Text, Text, Text> {

        private Text outK = new Text(); // Create a Text object for output key
        private Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                outK.set(records[0]);
                outV.set(records[1] + ", " + records[2]);
                context.write(outK, outV);
            }
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        private Text outV = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            String user_info = "";

            boolean counter = false;
            for (Text value : values) {
                if (value.toString().contains(",")) {
                    user_info = value.toString();
                } else {
                    if (!value.toString().equals("1")) {
                        throw new IOException("Value is not 1, but " + value.toString());
                    }
                    sum += Integer.parseInt(value.toString());
                    counter = true;
                }
            }
            // write out according to mapper type
            if (!counter) {
                outV.set(user_info);
            } else {
                outV.set(String.valueOf(sum));
            }
            // System.out.println("debug: " + key + ", " + String.valueOf(sum) + ", " + user_info); // for debug!

            // write out the result (output (key, value) pair of reduce phase)
            context.write(key,outV);
        }
    }

    public static class JobReducer_All extends Reducer<Text, Text, Text, Text> {
        private Text outV = new Text();
        int[] top_visit = {0,0,0,0,0,0,0,0,0,0};
        String[] top_user = {"","","","","","","","","",""};
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // get values length
            String user_info = "";
            int user_visit = 0;
            for (Text value : values) {
                if (value.toString().contains(",")) {
                    user_info = value.toString();
                } else {
                    user_visit += Integer.parseInt(value.toString().trim());
                }
            }
            // System.out.println("debug: " + key + ", " + user_visit + ", " + user_info);
            // preserve top10 visit users
            // find out the minimum visit count in the top_visit array
            int min_index = 0;
            int min_visit = Integer.MAX_VALUE;
            int i = 0;
            for (i = 0; i < top_visit.length; i++) {
                if (top_visit[i] < min_visit) {
                    min_index = i;
                    min_visit = top_visit[i];
                }
            }
            if (user_visit > top_visit[min_index]) {
                top_visit[min_index] = user_visit;
                top_user[min_index] = user_info;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // System.out.println("top_visit.length: " + top_visit[0] + ", " + top_visit[1]);
            // System.out.println("top_visit.length: " + top_user[0]);
            // write out the top 10 users
            for (int i = 0; i < top_visit.length; i++) {
                if (!top_user[i].isEmpty()) {
                    context.write(new Text(top_user[i]), new Text(String.valueOf(top_visit[i])));
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
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/ActivityLog.csv"), TextInputFormat.class, JobMapper_Activity.class); // second mapper
        // job.setMapperClass(JobMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(JobReducer_All.class);

        // 3. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 4. Specify the input and output path
        // FileInputFormat.setInputPaths(job, new Path("/home/zhiyang/data/CircleNetPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/zhiyang/output/project3b/"));

        job.setNumReduceTasks(1); // important to have a single reducer for top 10 calculation

        // 5. submit job!
        boolean result = job.waitForCompletion(true);
        
        System.exit(result ? 0 : 1);
    }
}