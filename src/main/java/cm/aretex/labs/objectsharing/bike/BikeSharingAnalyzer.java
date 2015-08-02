package cm.aretex.labs.objectsharing.bike;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cotdp.hadoop.ZipFileInputFormat;



/**
 * Hello world!
 *
 */
public class BikeSharingAnalyzer extends Configured implements Tool {
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(true), new BikeSharingAnalyzer(), args);
		System.out.println("BikeSharingAnalyzer Exit Code = " + exitCode);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		if (conf == null)
			conf = new Configuration(true);

		conf.setStrings("InputDirectory", args[0]);
		conf.setStrings("OutputDirectory", args[1]);
		conf.set("mapreduce.output.textoutputformat.separator", ";"); 

		if (System.getProperty("oozie.action.conf.xml") != null) {
			conf.addResource(new Path(System
					.getProperty("oozie.action.conf.xml")));
		}

		//Job job = new Job(conf);
		Job job = Job.getInstance(conf);
		job.setJobName(this.getClass().getSimpleName());
		job.setJarByClass(BikeSharingAnalyzer.class);
		
		job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);  

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
        
		job.setMapperClass(BikeSharingMapper.class);
		job.setReducerClass(BikeSharingReducer.class);

        ZipFileInputFormat.setLenient( true );
		ZipFileInputFormat.setInputPaths(job, new Path(conf.get("InputDirectory")));
        TextOutputFormat.setOutputPath(job, new Path(conf.get("OutputDirectory")));
        
        
        //BaselineSingleUtils.createOutputFolder(outputlocation, conf)
        //MultipleOutputs.addNamedOutput(job, "bike_tripduration_stats", TextOutputFormat.class, Text.class, Text.class);
        //MultipleOutputs.addNamedOutput(job, "bike_tripduration_distrib", TextOutputFormat.class, Text.class, Text.class);
        

        

		boolean success = job.waitForCompletion(true);

		if (success) {
			/*
			 * Print out the counters that the mappers have been incrementing.
			 */

			long nTrips = job.getCounters().findCounter("BikeParams", "nTrips")
					.getValue();
			long nStations = job.getCounters().findCounter("BikeParams", "nStations")
					.getValue();
			long nBikes = job.getCounters().findCounter("BikeParams", "nBikes")
					.getValue();
			
			ArrayList<String> counters = new ArrayList<String>();
			counters.add("nTrips"+";"+nTrips);
			counters.add("nStations"+";"+nStations);
			counters.add("nBikes"+";"+nBikes);
			
			BaselineSingleUtils.writeLines(new Path(conf.get("OutputDirectory")), conf, counters, "BikeParams");
			return 0;
		} 
		else
			return 1;
	}
}
