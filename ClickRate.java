//Brianna, Lester, Mary
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.*;
import org.json.simple.parser.*;


public class ClickRate{
    /*
     * MAP:
     * Read from both files looking @ the impression ID
     * Check if it's a click or impression
     * <impressionID, click/impression>
     * REDUCE:
     * <impressionID click, total freq> and <impressionID impression, total freq>
     * REDUCE:
     * <impressionID, total click / total freq> 
     * ~~
     * And then make a matrix with axes: URL, adID
     * Matrix[URL][adID] = # of clicks / # of impressions
     */
    static final Path TEMP_OUTPUT_FILE = new Path("temp/temp.txt");
    public static void main(String[] args){
	if (args.length < 3){
	    System.err.println("Wrong number of parameters.");
	    System.err.println("Expected: [impression file] [click file] [output file]");
	    System.exit(1);
	}

	try{
	    Configuration conf = new Configuration();
	    
	    Job job1 = Job.getInstance(conf, "Phase1");
	    job1.setJarByClass(ClickRate.class);
	    
	    job1.setMapperClass(ClickRate.TypeMapper.class);
	    job1.setReducerClass(ClickRate.FrequencyReducer.class);
	    
	    // Pass in impression/click files as input and
	    // output to a temporary file
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileInputFormat.addInputPath(job1, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job1, TEMP_OUTPUT_FILE);

	    job1.waitForCompletion(true);

	    Job job2 = Job.getInstance(conf, "Phase2");
	    job2.setJarByClass(ClickRate.class);

	    job2.setReducerClass(ClickRate.RateReducer.class);

	    // Pass in temp file and output to specified file
	    FileInputFormat.addInputPath(job2, TEMP_OUTPUT_FILE);
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}catch (IOException e){
	    System.err.println("Invalid file.");
	    e.printStackTrace();
	}catch (Exception e){
	    e.printStackTrace();
	}
    }

    /*IMPORTANT: ImpressionID is a string of the format
     * "IMPRESSIONID_REFERRER_ADID"*/
    
    public static class TypeMapper
	extends Mapper <Object, Text, Text, Text>{
	/*
	 * TypeMapper reads in both impressionlog & click log
	 * For each JSON object: { text }
	 * Find impression ID
	 * Check if Impression (look for referrer) or click
	 * Output <Impression ID, Click> or
	 * Output <Impression ID, Impression> 
	 */
	public void map(Object key, Text value, Context context)
	    throws IOException, InterruptedException {
	    //value is a line in the inputfiles specified above
	    String jsonstring = value.toString(); // assume this is json
	    JSONParser parser = new JSONParser();
	    Text id = new Text();
	    Text type = new Text();
	    try{
		Object obj = parser.parse(jsonstring);
		JSONObject json = (JSONObject) obj;
		System.out.println(json);
		String adID = (String) json.get("adId");
		String impressionID = (String) json.get("impressionId");
		String outKey = adID+","+impressionID;
		id.set(outKey);
		if (json.containsKey("referrer")){ // is this entry an impression?
		    String referrer = (String) json.get("referrer");
		    type.set(referrer);
		}else{
		    type.set("click");
		}
		context.write(id, type);
	    }catch(Exception e){
		e.printStackTrace();
	    }
	}
    }

    public static class FrequencyReducer
	extends Reducer <Text, Text, Text, IntWritable> {
	/*
	  Takes in a bunch of <Impression ID, Click/Impression>
	  Amasses the frequencies for each Impression ID by incrementing
	  And outputs:
	  <ImpressionID_Click, total # of IDclicks>
	  <ImpressionID_Impression, total # of IDimpressions>
	*/
    }

    public static class RateReducer
	extends Reducer <Text, IntWritable, Text, LongWritable>{
	/*
	  Takes in <ImpressionID_Click, freq> and 
	  <ImpressionID_Impression, freq>
	  and outputs
	  <Referrer_AdID, clickfreq / impressionfreq>
	*/
    }
}
