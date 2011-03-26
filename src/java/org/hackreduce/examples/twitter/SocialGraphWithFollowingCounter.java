package org.hackreduce.examples.twitter;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This MapReduce job will count the total number of Bixi records in the data dump.
 *
 */
public class SocialGraphWithFollowingCounter extends Configured implements Tool {


	public enum Count {
		RECORDS_SKIPPED,
		TOTAL_KEYS,
		UNIQUE_KEYS
	}

	public static class SocialGraphWithFollowingCounterMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

		@Override
		@SuppressWarnings("unused")
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String inputString = value.toString();
			long userID;
			long followerID;

			try {
				// This code is copied from the constructor of StockExchangeRecord

				String[] attributes = inputString.split("\t");

				if (attributes.length != 2)
					throw new IllegalArgumentException("Input string given did not have 2 values in TSV format");

				try {
					userID     = Integer.parseInt(attributes[0]);
					followerID = Integer.parseInt(attributes[1]);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed");
				}
			} catch (Exception e) {
				context.getCounter(Count.RECORDS_SKIPPED).increment(1);
				return;
			}

		    context.getCounter(Count.TOTAL_KEYS).increment(1);
			context.write(new LongWritable(followerID), new LongWritable(userID));
		}
	}
	public static class SocialGraphWithFollowingCounterReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			context.getCounter(Count.UNIQUE_KEYS).increment(1);
			long count = 0;
			ArrayList<LongWritable> users = new ArrayList<LongWritable>();
			for (LongWritable value : values) {
				users.add(new LongWritable(value.get()));
				count += 1;
			}
			for (LongWritable user : users){
				context.write(user, new Text("" + key.get() + "\t" + count + "\t" + "0.00001"));
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

        if (args.length != 2) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output>");
        	System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName("Very secret world domination project");

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(SocialGraphWithFollowingCounterMapper.class);
		job.setReducerClass(SocialGraphWithFollowingCounterReducer.class);

		// The Nasdaq/NYSE data dumps comes in as a CSV file (text input), so we configure
		// the job to use this format.
		job.setInputFormatClass(TextInputFormat.class);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

        // // This is what the Reducer will be outputting
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new SocialGraphWithFollowingCounter(), args);
		System.exit(result);
	}

}
