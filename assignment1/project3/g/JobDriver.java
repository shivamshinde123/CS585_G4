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
import java.util.HashMap;
import java.util.Map;

public class JobDriver {
    // Mapper for CircleNetPage to get user IDs and nicknames
    public static class JobMapper_NetPage extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable outK = new IntWritable();
        private Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] records = line.split(",");
            
            if (key.get() > 0) { // Skip header
                int userId = Integer.parseInt(records[0]);
                String nickname = records[1];
                
                outK.set(userId);
                outV.set("USER:" + nickname);
                context.write(outK, outV);
            }
        }
    }

    // Mapper for ActivityLog to track most recent activity
    public static class JobMapper_Activity extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable outK = new IntWritable();
        private Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] records = line.split(",");
            
            if (key.get() > 0) { // Skip header
                int userId = Integer.parseInt(records[1]);
                int actionTime = Integer.parseInt(records[4]);
                
                outK.set(userId);
                // Use ACTION: prefix to distinguish from user data
                outV.set("ACTION:" + actionTime);
                context.write(outK, outV);
            }
        }
    }

    // Reducer to find outdated pages
    public static class JobReducer_All extends Reducer<IntWritable, Text, IntWritable, Text> {
        private static final int OUTDATED_THRESHOLD = 90; // 90 days without activity

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            String nickname = null;
            int mostRecentAction = 0;

            // Process all values for this user
            for (Text value : values) {
                String valueStr = value.toString();
                
                if (valueStr.startsWith("USER:")) {
                    // Extract nickname
                    nickname = valueStr.substring(5);
                } else if (valueStr.startsWith("ACTION:")) {
                    // Track most recent action time
                    int actionTime = Integer.parseInt(valueStr.substring(7));
                    mostRecentAction = Math.max(mostRecentAction, actionTime);
                }
            }

            // Check if user is outdated (no activity in last 90 days)
            // Assuming current timestamp is 1,000,000
            if (nickname != null && (1_000_000 - mostRecentAction > OUTDATED_THRESHOLD)) {
                context.write(key, new Text(nickname));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Outdated CircleNet Pages");
        
        job.setJarByClass(JobDriver.class);
        
        // Multiple input paths
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/CircleNetPage.csv"), 
                                    TextInputFormat.class, JobMapper_NetPage.class);
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/ActivityLog.csv"), 
                                    TextInputFormat.class, JobMapper_Activity.class);
        
        job.setReducerClass(JobReducer_All.class);
        
        // Output configuration
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path("/home/zhiyang/output/project3g/"));
        
        job.setNumReduceTasks(1);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}