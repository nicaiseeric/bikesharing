package cm.aretex.labs.objectsharing.bike;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BikeSharingMapper extends Mapper<Text, BytesWritable, Text, Text> {
	
	/*private enum nCounter {
		 nTrips, nStations
	} */
    public void map( Text key, BytesWritable value, Context context )
        throws IOException, InterruptedException
    {
        // NOTE: the filename is the *full* path within the ZIP file
        // e.g. "subdir1/subsubdir2/Ulysses-18.csv"
        String filename = key.toString();
        
        // We only want to process .csv files
        if ( filename.endsWith(".csv") == false )
            return;
        
        // Prepare the content 
        String content = new String( value.getBytes(), "UTF-8" );
        //content = content.replaceAll( "[\"]", "" ).toLowerCase();
        
        // Tokenize the content
        StringTokenizer tokenizer = new StringTokenizer(content, "\n");
        while ( tokenizer.hasMoreTokens() ) {
        	String line = tokenizer.nextToken();
        	if(line.contains("tripduration") || line.contains("\\N") )
        		continue;
        	
        	String[] lineItems = line.replaceAll("\"", "").split(",");
        	
        	if(lineItems.length < 15)
        		continue;
        	
        	//context.getCounter(nCounter.nTrips).increment(1);
        	context.getCounter("BikeParams", "nTrips").increment(1);
        	
        	
        	// emit trip duration
        	String time = lineItems[0];
        	context.write( new Text("time_"), new Text(time)); 
        	
        	String startstationID = lineItems[3];
        	String endstationID = lineItems[7];
        	

        	context.write( new Text("station_"+startstationID), new Text("1"));
        	context.write( new Text("station_"+endstationID), new Text("1"));
        	
        	// emit starttime and stoptime: format "2013-10-01 00:01:08"
        	//String sday = lineItems[1].split(" ")[0];        	
        	String sts = "";
        	//String eday = lineItems[2].split(" ")[0];
        	String ets = "";
        	
        	
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			try {
				Date startDate = formatter.parse(lineItems[1]);
				Date endDate = formatter.parse(lineItems[2]);
				
				sts = ""+startDate.getTime();
				ets = ""+endDate.getTime();
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if(!sts.isEmpty())
				context.write( new Text("tsstart_"+startstationID+"_"+sts), new Text("1"));
			if(!ets.isEmpty())
				context.write( new Text("tsend_"+endstationID+"_"+ets), new Text("1"));
			
						        	
        	// emit trip distance
        	double xi = Double.parseDouble(lineItems[5]);
        	double yi = Double.parseDouble(lineItems[6]);
        	double xj = Double.parseDouble(lineItems[9]);
        	double yj = Double.parseDouble(lineItems[10]);
        	
        	//double dist = Math.sqrt( Math.pow( (xi-xj), 2) + Math.pow( (yi-yj), 2) );
        	double dist = distance(xi, yi, xj, yj);
        	if(Double.isNaN(dist))
        		continue;
        	context.write( new Text("dist_"), new Text(""+dist));
        	
        	String bikeID = lineItems[11];
        	context.write( new Text("bike_"+bikeID), new Text(time+"_"+dist+"_1"));   

        	String ageBehavior = "" + (2015 - Integer.parseInt(lineItems[13]));
        	String genderBehavior = lineItems[14];
        	
        	context.write( new Text("routing_"+startstationID+"_"+endstationID), new Text(time+"_"+dist+"_"+ageBehavior+"_"+genderBehavior+"_1"));
        	//context.write( new Text("routing_"+startstationID+"_"+endstationID), new Text(time+"_"+dist+"_"+age+"_"+gender+"_1"));
        	
        	    	
        	
        			
        	
        }
    }
    
	private double distance(double lat1, double lon1, double lat2, double lon2) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
				* Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		dist = dist * 1.609344;
		return (dist);
	}

	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	/* :: This function converts decimal degrees to radians : */
	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	private double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	/* :: This function converts radians to decimal degrees : */
	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	private double rad2deg(double rad) {
		return (rad * 180 / Math.PI);
	}

	

}
