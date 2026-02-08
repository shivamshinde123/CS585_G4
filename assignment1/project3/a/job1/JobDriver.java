import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobDriver {
    public static class JobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text outK = new Text(); // Create a Text object for output key
        private IntWritable outV = new IntWritable(1); // Create a IntWritable for output value

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) {
                outK.set(records[4]);
                context.write(outK, outV);
            }
        }
    }

    public static class JobReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
        private IntWritable outV = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            // get the total count of word occurrences for each word (input key)
            for (IntWritable value : values) {
                sum += value.get(); // The data type of value is already IntWritable, to get its value, we need to call a getter
            }
            outV.set(sum);

            // write out the result (output (key, value) pair of reduce phase)
            context.write(key,outV);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. map the classes
        job.setJarByClass(JobDriver.class);
        job.setMapperClass(JobMapper.class);
        job.setCombinerClass(JobReducer.class);
        job.setReducerClass(JobReducer.class);

        // 3. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4. Specify the input and output path
        FileInputFormat.setInputPaths(job, new Path("/home/zhiyang/data/CircleNetPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/zhiyang/output/project3a/"));

        job.setNumReduceTasks(1);

        // 5. submit job!
        boolean result = job.waitForCompletion(true);
        
        System.exit(result ? 0 : 1);
    }
}