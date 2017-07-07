
//package wiki.hadoopcomposite;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WikiMain {

	/**
	 * Krishna - for processing XML file using Hadoop MapReduce
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();
			// conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 2048);

			// OR alternatively you can set it this way, the name of the
			// property is
			// "mapreduce.input.fixedlengthinputformat.record.length"
			// conf.setInt("mapreduce.input.fixedlengthinputformat.record.length",
			// 2048);
			String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();

			conf.set("START_TAG_KEY", "<revision>");
			conf.set("END_TAG_KEY", "</revision>");
			// conf.set("mapred.min.split.size", "536800000");
			// conf.set("mapred.max.split.size", "53680");
			// Job job = new Job(conf, "XML Processing Processing");
			Job job = Job.getInstance(conf);
			job.setJarByClass(WikiMain.class);
			job.setMapperClass(EditSession.class);
			job.setReducerClass(MyReducer2.class);
			job.setPartitionerClass(SessionPartitioner.class);
			
			job.setGroupingComparatorClass(KeyGrouping.class);
			job.setSortComparatorClass(CompositeKeyComparator.class);

			FileSystem fs = FileSystem.get(conf);
			job.setNumReduceTasks(1);

			job.setInputFormatClass(XmlInputFormat.class);
			// job.setOutputValueClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(CompositeKey.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path input = new Path(args[0]);
			Path output = new Path(args[1]);
			/*
			 * if(!fs.exists(input)) {
			 * System.err.println("Input file doesn't exists"); return; }
			 * if(fs.exists(output)) { fs.delete(output, true);
			 * System.err.println("Output file deleted"); } fs.close();
			 */
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);
			job.waitForCompletion(true);

		} catch (Exception e) {
			// LogWriter.getInstance().WriteLog("Driver Error: " +
			// e.getMessage());
			System.out.println(e.getMessage().toString());
		}
		// job.setReducerClass(ClickReducer.class);

	}
}
