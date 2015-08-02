package cm.aretex.labs.objectsharing.bike;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

//import cm.aretex.labs.objectsharing.bike.BikeSharingMapper.nCounter;

import com.tdunning.math.stats.TDigest;

public class BikeSharingReducer extends Reducer<Text, Text, Text, Text>{
	
	/*private enum nCounter {
		 nTrips, nStations, nBikes
	}*/
	
	private MultipleOutputs<Text, Text> mos;
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		//write counters before close "mos"
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String strKey = key.toString();
		if(strKey.startsWith("time_")){
			SummaryStatistics sstats = new SummaryStatistics();
			TDigest digest = TDigest.createDigest(100);
			
			for (Text val : values) {
				sstats.addValue(Double.parseDouble(val.toString()));
				digest.add(Double.parseDouble(val.toString()));				
			}
			
			for(int i = 0; i<100; i++){
				mos.write(new Text(""+digest.quantile(i/100.0)), new Text(""+i/100.0), "bike_tripduration_distrib"+"/bike_tripduration_distribdata");
				//mos.write(new Text(""+digest.quantile(i/100.0)), new Text(""+i/100.0), context.getConfiguration().get("OutputDirectory")+"/bike_tripduration_distrib");
				//mos.write("cdftripduration", new Text(""+digest.quantile(i/100.0)), new Text(""+i/100.0), context.getConfiguration().get("OutputDirectory")+"/bike_tripduration_distrib");
				//mos.write("bike_tripduration_distrib", new Text(""+digest.quantile(i/100.0)), new Text(""+i/100.0));
			}
			

			outputKey.clear();
			outputKey.set(strKey.replaceAll("_", ""));

			outputValue.clear();
			String strOut = "nSamples="+sstats.getN()+";"+"Mean="+sstats.getMean()+";"+"Std="+sstats.getStandardDeviation()+";"+"Max="+sstats.getMax()+";"+"Min="+sstats.getMin();
			outputValue.set(strOut);
			mos.write(outputKey, outputValue, "bike_tripduration_stats"+"/bike_tripduration_statsdata");
			//mos.write(outputKey, outputValue, context.getConfiguration().get("OutputDirectory")+"/bike_tripduration_stats");
			//mos.write("tripduration", outputKey, outputValue, context.getConfiguration().get("OutputDirectory")+"/bike_tripduration_stats");			
			//mos.write("bike_tripduration_stats", outputKey, outputValue);			
		}
		
		if(strKey.startsWith("dist_")){
			SummaryStatistics sstats = new SummaryStatistics();
			TDigest digest = TDigest.createDigest(100);
			
			for (Text val : values) {
				sstats.addValue(Double.parseDouble(val.toString()));
				digest.add(Double.parseDouble(val.toString()));				
			}
			
			for(int i = 0; i<100; i++){
				mos.write(new Text(""+digest.quantile(i/100.0)), new Text(""+i/100.0), "bike_distance_distrib"+"/bike_distance_distribdata");
				//mos.write(new Text(""+digest.quantile(i/100.0)), new Text(""+i/100.0), context.getConfiguration().get("OutputDirectory")+"/bike_distance_distrib");
				//mos.write("cdfdistance", new Text(""+digest.quantile(i/100.0)), new Text(""+i/100.0), context.getConfiguration().get("OutputDirectory")+"/bike_distance_distrib");
				//mos.write("bike_tripduration_distrib", new Text(""+digest.quantile(i/100.0)), new Text(""+i/100.0));
			}
			

			outputKey.clear();
			outputKey.set(strKey.replaceAll("_", ""));

			outputValue.clear();
			String strOut = "nSamples="+sstats.getN()+";"+"Mean="+sstats.getMean()+";"+"Std="+sstats.getStandardDeviation()+";"+"Max="+sstats.getMax()+";"+"Min="+sstats.getMin();
			outputValue.set(strOut);
			mos.write(outputKey, outputValue, "bike_distance_stats"+"/bike_distance_statsdata");
			//mos.write(outputKey, outputValue, context.getConfiguration().get("OutputDirectory")+"/bike_distance_stats");
			//mos.write("distance", outputKey, outputValue, context.getConfiguration().get("OutputDirectory")+"/bike_distance_stats");			
			//mos.write("bike_tripduration_stats", outputKey, outputValue);			
		}
		
		if(strKey.startsWith("station_")){
        	//context.getCounter(nCounter.nStations).increment(1);
        	context.getCounter("BikeParams", "nStations").increment(1);
        	
			String strStationID = strKey.split("_")[1];	
			//String strGlobalPopularity = "";
			int globalPopularity = 0;
			for (Text val : values) {
				globalPopularity++;				
			}

			mos.write(new Text(strStationID), new Text(""+globalPopularity), "bike_station_popularity"+"/bike_station_globalpopularity");
			//mos.write("allstations", new Text(strStationID), new Text(""+globalPopularity), context.getConfiguration().get("OutputDirectory")+"/bike_station_globalpopularity");			
			//mos.write("bike_station_globalpopularity", new Text(strStationID), new Text(""+globalPopularity));
			
		}
		

		if(strKey.startsWith("bike_")){
        	//context.getCounter(nCounter.nBikes).increment(1);
        	context.getCounter("BikeParams", "nBikes").increment(1);
        	
			String strBikeID = strKey.split("_")[1];	
			int utilisationFrequency = 0;
			SummaryStatistics stimestats = new SummaryStatistics();
			SummaryStatistics sdiststats = new SummaryStatistics();
			
			for (Text val : values) {
				String[] valItems = val.toString().split("_");
				stimestats.addValue(Double.parseDouble(valItems[0]));

				sdiststats.addValue(Double.parseDouble(valItems[1]));
				
				utilisationFrequency++;				
			}

			mos.write(new Text(strBikeID), new Text(stimestats.getMean()+";"+sdiststats.getMean()+";"+utilisationFrequency), "bike_utilization"+"/bike_utilization_stats");
			//mos.write("allbikes", new Text(strBikeID), new Text(stimestats.getMean()+";"+sdiststats.getMean()+";"+utilisationFrequency), context.getConfiguration().get("OutputDirectory")+"/bike_utilization");			
			//mos.write("bike_station_globalpopularity", new Text(strStationID), new Text(""+globalPopularity));
			
		}
		

		if(strKey.startsWith("tsstart_")){
			String strStationID = strKey.split("_")[1];	
			String strTimeStamp = strKey.split("_")[2];	
			//String strGlobalPopularity = "";
			int localPopularity = 0;
			for (Text val : values) {
				localPopularity++;				
			}

			mos.write(new Text(strTimeStamp), new Text(""+localPopularity), "bike_station_"+strStationID+"/bike_station"+strStationID+"_startpopularity");
			//mos.write(new Text(strTimeStamp), new Text(""+localPopularity), context.getConfiguration().get("OutputDirectory")+"/bike_station"+strStationID+"_startpopularity");
			//mos.write(strStationID, new Text(strTimeStamp), new Text(""+localPopularity), context.getConfiguration().get("OutputDirectory")+"/bike_station"+strStationID+"_startpopularity");
			//mos.write("bike_startstation_localpopularity", new Text(strStationID), new Text(""+localPopularity));
			
		}

		if(strKey.startsWith("tsend_")){
			String strStationID = strKey.split("_")[1];	
			String strTimeStamp = strKey.split("_")[2];	
			//String strGlobalPopularity = "";
			int localPopularity = 0;
			for (Text val : values) {
				localPopularity++;				
			}

			mos.write(new Text(strTimeStamp), new Text(""+localPopularity), "bike_station_"+strStationID+"/bike_station"+strStationID+"_endpopularity");
			//mos.write(new Text(strTimeStamp), new Text(""+localPopularity), context.getConfiguration().get("OutputDirectory")+"/bike_station"+strStationID+"_endpopularity");
			//mos.write(strStationID, new Text(strTimeStamp), new Text(""+localPopularity), context.getConfiguration().get("OutputDirectory")+"/bike_station"+strStationID+"_endpopularity");
			//mos.write("bike_startstation_localpopularity", new Text(strStationID), new Text(""+localPopularity));
			
		}
		
		if(strKey.startsWith("routing_")){
			String strStartStationID = strKey.split("_")[1];	
			String strEndStationID = strKey.split("_")[2];
			
			SummaryStatistics stimestats = new SummaryStatistics();
			SummaryStatistics sdiststats = new SummaryStatistics();
			SummaryStatistics sagestats = new SummaryStatistics();
			
			int countMales = 0;
			int countFemales = 0;
			int localPopularity = 0;
			
			
			for (Text val : values) {
				String[] valItems = val.toString().split("_");	
				stimestats.addValue(Double.parseDouble(valItems[0]));

				sdiststats.addValue(Double.parseDouble(valItems[1]));
				
				sagestats.addValue(Double.parseDouble(valItems[2]));
				
				if(valItems[3].equalsIgnoreCase("1"))
					countMales++;
				if(valItems[3].equalsIgnoreCase("2"))
					countFemales++;
					
				
				localPopularity++;				
				
			}
			mos.write(new Text(strStartStationID), new Text(strEndStationID+";"+stimestats.getMean()+";"+sdiststats.getMean()+";"+countMales+";"+countFemales+";"+localPopularity), "bike_routing/bike_routing_matrix");
			//mos.write(new Text(strStartStationID), new Text(strEndStationID+";"+stimestats.getMean()+";"+sdiststats.getMean()+";"+countMales+";"+countFemales+";"+localPopularity), context.getConfiguration().get("OutputDirectory")+"/bike_routing_matrix");			
			//mos.write("allstations", new Text(strStartStationID), new Text(strEndStationID+";"+stimestats.getMean()+";"+sdiststats.getMean()+";"+countMales+";"+countFemales+";"+localPopularity), context.getConfiguration().get("OutputDirectory")+"/bike_routing_matrix");
			//mos.write("bike_startstation_localpopularity", new Text(strStationID), new Text(""+localPopularity));
			
		}
		
		
		
	}

}
