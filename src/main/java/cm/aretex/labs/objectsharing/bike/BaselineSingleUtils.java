package cm.aretex.labs.objectsharing.bike;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class BaselineSingleUtils {
	
	public final static String metricNaNvalue = "nan";
	public final static String timeseriesDateFormat = "dd/MM/yyyy HH:mm:ss";
	public final static String reportSeparator = "\r\n";
	//public final static String workingDaySeparator = "-";
	public final static String metricSeparator = ";";
	//public final static String keyvalueSeparator = ",|\t";
	public final static String keyvalueSeparator = ",";
	public final static int numfields = 3;
	/*
	public static void main(String[] args) {
		
		System.out.println(formatDateFolder(null, true));
		
	}
	*/
	
	public static void printErrorMessage(String str) throws IOException{
		System.err.println(str);
		throw new IOException(str);
		//System.exit(1);
	}
	
	
	public static boolean deleteOutputFolder(Path outputlocation, Configuration conf) throws Exception{		
		FileSystem fileSystem = FileSystem.get(outputlocation.toUri(), conf);
		if (fileSystem.exists(outputlocation)) 
			return fileSystem.delete(outputlocation, true);
				
		return false;
	}
	
	public static boolean createOutputFolder(Path outputlocation, Configuration conf) throws Exception{		
		FileSystem fileSystem = FileSystem.get(outputlocation.toUri(), conf);
		if (fileSystem.exists(outputlocation)) 
			return true;
		
		fileSystem.mkdirs(outputlocation);
		FileStatus[] items = fileSystem.listStatus(outputlocation);
		if (items == null)
			return true;		
		return false;
	}
	
	public static void cleanOutputFolder(Path outputlocation, Configuration conf) throws Exception{		
		FileSystem fileSystem = FileSystem.get(outputlocation.toUri(), conf);
		FileStatus[] items = fileSystem.listStatus(outputlocation);
		if (items != null)
			for (FileStatus item : items) 
				fileSystem.delete(item.getPath(), true);
	}
	

	public static Hashtable<String, ArrayList<String>> ArraylistToHashtableOfString(ArrayList<String> arrayList, String keyvalueseparator, String metricseparator) {
		Hashtable<String, ArrayList<String>> hashTable = new Hashtable<String, ArrayList<String>>();
				
		Iterator<String> it = arrayList.iterator();
		
		if(keyvalueseparator == null)
			keyvalueseparator = BaselineSingleUtils.keyvalueSeparator;

		if(metricseparator == null)
			metricseparator = BaselineSingleUtils.metricSeparator;
		
		while(it.hasNext()){
			String[] items = it.next().split(keyvalueseparator);
			String key = items[0];
			ArrayList<String> value = new ArrayList<String>();
			if(items.length > 1)
				for(String item : items[1].split(metricseparator))
					value.add(item);
			
			hashTable.put(key, value);
		}		
				
		return hashTable;
	}


	public static ArrayList<String> HashtableToArraylistOfString(Hashtable<String, ? extends List<String>> hashTable, String keyvalueseparator, String metricseparator) {

		if(keyvalueseparator == null)
			keyvalueseparator = BaselineSingleUtils.keyvalueSeparator;

		if(metricseparator == null)
			metricseparator = BaselineSingleUtils.metricSeparator;
		
		ArrayList<String> arraylist = new ArrayList<String>();
		Enumeration<String> listkeys = hashTable.keys();
		String key = "";
		String value = "";
		while (listkeys.hasMoreElements()) {
			key = listkeys.nextElement();
			for (String listitem : hashTable.get(key))
				value += (metricseparator + listitem);
			//value = key + "," + value.replaceFirst(";", "");
			//value = key + value.replaceFirst(metricSeparator, keyvalueSeparator);
			value = key + value.replaceFirst(metricseparator, keyvalueseparator);
			arraylist.add(value);
			value = "";
		}
		return arraylist;
	}

	public static boolean writeLines(Path location, Configuration conf,
			ArrayList<String> results, String fileName) throws IOException {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		// CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		//FileStatus[] items = fileSystem.listStatus(location);
		
		/*
		if (items != null && items.length > 0) {
			printErrorMessage("Output Directory already exists and is not empty!\n" + items.length + " files/directories are present!");
			return false;
		}*/
		
		if(!fileName.startsWith("/"))
			fileName = "/" + fileName;

		Path outFile = location.suffix(fileName);

		// OutputStream stream = fileSystem.create(outFile);
		if (fileSystem.exists(outFile)) 
			fileSystem.delete(outFile, true);
		
		FSDataOutputStream stream = fileSystem.create(outFile);
		
		
		IOUtils.writeLines(results, IOUtils.LINE_SEPARATOR, stream, "UTF-8");
		
		
		//BufferedWriter br = new BufferedWriter( new OutputStreamWriter( stream, "UTF-8" ) );
				
		//for(String line : results){ 
			//IOUtils.write(line, stream, "UTF-8"); 
			//br.write(line);
		//}
		//br.flush();
		//br.close();
		
		
		IOUtils.closeQuietly(stream);
		
		
		return fileSystem.exists(outFile);
	}
		
	public static ArrayList<String> readLines(Path location, Configuration conf)
			throws IOException {
		/*if(location.toUri() == null)
			printErrorMessage("NULLLL Location URI");
		if(conf == null)
			printErrorMessage("NULLLL CONF");*/
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		FileStatus[] items = fileSystem.listStatus(location);
		if (items == null)
			return new ArrayList<String>();

		ArrayList<String> results = new ArrayList<String>();
		for (FileStatus item : items) {

			// ignoring files like _SUCCESS
			if (item.getPath().getName().startsWith("_") || item.getPath().getName().startsWith(".") ) {
				continue;
			}

			CompressionCodec codec = factory.getCodec(item.getPath());
			InputStream stream = null;

			// check if we have a compression codec we need to use
			if (codec != null) {
				stream = codec
						.createInputStream(fileSystem.open(item.getPath()));
			} else {
				stream = fileSystem.open(item.getPath());
			}

			StringWriter writer = new StringWriter();
			IOUtils.copy(stream, writer, "UTF-8");
			String raw = writer.toString();
			// String[] resulting = raw.split("\n");
			for (String str : raw.split("\n")) {
				results.add(str);
			}
		}
		return results;
	}
	
}
