import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobDriver {
    public static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outK = new Text(); // Create a Text object for output key
        private Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                if (records[4].equals("Photography")) {
                    String nickname = records[1];
                    String job = records[2];
                    outK.set(nickname);
                    outV.set(job);
                    context.write(outK, outV);
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
        job.setMapperClass(JobMapper.class);
        // job.setCombinerClass(Combiner.class);
        // job.setReducerClass(JobReducer_All.class);

        // 3. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        // 4. Specify the input and output path
        FileInputFormat.setInputPaths(job, new Path("/home/zhiyang/data/CircleNetPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/zhiyang/output/project3c/"));

        // job.setNumReduceTasks(1); // important to have a single reducer for top 10 calculation

        // 5. submit job!
        boolean result = job.waitForCompletion(true);
        
        System.exit(result ? 0 : 1);
    }
}