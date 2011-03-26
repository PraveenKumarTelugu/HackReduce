package org.hackreduce.examples.twitter;

import java.io.IOException;
import java.text.ParseException;

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

// import org.hackreduce.models.FollowRecord;


/**
 * This MapReduce job will count the total number of Bixi records in the data dump.
 *
 */
public class FollowersCounter extends Configured implements Tool {

	public static class FollowersCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		public static final Text TOTAL_COUNT = new Text("total");

		// Just to save on object instantiation
		public static final LongWritable ONE_COUNT = new LongWritable(1);

    	public enum Count {
    		RECORDS_SKIPPED,
    		TOTAL_KEYS,
    		UNIQUE_KEYS
    	}

		@Override
		@SuppressWarnings("unused")
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String inputString = value.toString();
			String userID;
			String followerID;

			try {
				// This code is copied from the constructor of StockExchangeRecord

				String[] attributes = inputString.split("\t");

				if (attributes.length != 2)
					throw new IllegalArgumentException("Input string given did not have 2 values in TSV format");

				try {
					userID     = attributes[0];
					followerID = attributes[1];
					
//				} catch (ParseException e) {
//					throw new IllegalArgumentException("Input string contained an unknown value that couldn't be parsed");
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed");
				}
			} catch (Exception e) {
				context.getCounter(Count.RECORDS_SKIPPED).increment(1);
				return;
			}

			context.write(new Text(userID), ONE_COUNT);
		}
	}
	public static class FollowersCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			context.write(key, new LongWritable(count));
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
        job.setMapperClass(FollowersCounterMapper.class);
		job.setReducerClass(FollowersCounterReducer.class);

		// The Nasdaq/NYSE data dumps comes in as a CSV file (text input), so we configure
		// the job to use this format.
		job.setInputFormatClass(TextInputFormat.class);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

        // // This is what the Reducer will be outputting
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new FollowersCounter(), args);
		System.exit(result);
	}

}
