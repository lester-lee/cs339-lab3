//Brianna, Lester, Mary
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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

	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    
	    job1.setMapperClass(ClickRate.TypeMapper.class);
	    job1.setReducerClass(ClickRate.FrequencyReducer.class);

	    job1.setInputFormatClass(TextInputFormat.class);
	    // Pass in impression/click files as input and
	    // output to a temporary file
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileInputFormat.addInputPath(job1, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job1, TEMP_OUTPUT_FILE);
	    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	    // FileOutputFormat.setOutputPath(job1, new Path(args[2]));
	    job1.waitForCompletion(true);
	    
	    Job job2 = Job.getInstance(conf, "Phase2");
	    job2.setJarByClass(ClickRate.class);

	    job2.setReducerClass(ClickRate.RateReducer.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(DoubleWritable.class);

	    job2.setInputFormatClass(SequenceFileInputFormat.class);
	    // Pass in impression/click files as input and
	    // output to a temporary file
	    FileInputFormat.addInputPath(job2, TEMP_OUTPUT_FILE);
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    // Pass in temp file and output to specified file
	    //FileInputFormat.addInputPath(job1, TEMP_OUTPUT_FILE);
	    
	    
	  

	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}catch (IOException e){
	    System.err.println("Invalid file.");
	    e.printStackTrace();
	}catch (Exception e){
	    e.printStackTrace();
	}
    }

    /*IMPORTANT: ImpressionID is a string of the format
     * adId,ImpressionId
     */
    
    public static class TypeMapper
	extends Mapper <LongWritable, Text, Text, Text>{
	/*
	 * TypeMapper reads in both impressionlog & click log
	 * Output <[AdId,ImpressionId], click> or
	 * Output <[AdId,ImpressionId], referrer>
	 */
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    //QUESTION: IS JSONSTRING ACTUALLY READING IN A JSON OBJECT FROM FILE?
	    //value is a line in the inputfiles specified above
	    String jsonstring = value.toString(); // assume this is json?
	    // not sure if the above line actually is json tho
	    JSONParser parser = new JSONParser();
	    Text id = new Text();
	    Text type = new Text();
	    try{
		// get relevant info from the JSON log entry
		Object obj = parser.parse(jsonstring);
		JSONObject json = (JSONObject) obj;
		//System.out.println(json);
		String adID = (String) json.get("adId");
		String impressionID = (String) json.get("impressionId");
		String pageURL = (String) json.get("referrer");
		String outKey = adID+","+impressionID;
		//if impression, output is the referrer, else it's "click"
		String outValue = (json.containsKey("referrer")) ?
		    (String) json.get("referrer") : "click"; 
		//debugging:
		//System.out.println(outKey+":"+outValue);
		id.set(outKey);
		type.set(outValue);
		context.write(id, type);
	    }catch(Exception e){
		e.printStackTrace();
	    }
	}
    }

    public static class FrequencyReducer
	extends Reducer <Text, Text, Text, Text> {
	/*
	  Takes in a bunch of <Impression ID, Click/Impression>
	  Amasses the frequencies for each Impression ID by incrementing
	  And outputs:
	  <ImpressionID_Click, total # of IDclicks>
	  <ImpressionID_Impression, total # of IDimpressions>
	  QUESTION: WE SHOULD TRY TO COMBINE THESE SO THIS REDUCE GIVES OUT
	  <Referrer_AdID, ???? what goes here ????>
	*/
	public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	    // ASSUMING referrer is unique per impression_ad
	    int csum = 0; // click freq
	    int isum = 0; // impression freq
	    String referrer = "";
	    for (Text val : values){
		if (val.toString().equals("click")){
		    csum++;
		}else{
		    isum++;
		    referrer = val.toString();
		    //System.out.println(key.toString() + "," + referrer);
		}
	    }

	    String newKey = referrer + "," + key.toString().split(",")[0];
	    key.set(newKey);
	    //System.out.println(csum);
	    context.write(key, new Text(Integer.valueOf(csum).toString()));
	}
    }

     public static class RateReducer
	extends Reducer <Text, Text, Text, DoubleWritable>{
	/*
	  Takes in <ImpressionID_Click, freq> and 
	  <ImpressionID_Impression, freq>
	  MISTAKE: reduce only gets all the values for one specific key
	  So we can't combine the results of two different keys

	  and outputs
	  <Referrer_AdID, clickfreq / impressionfreq>
	*/
    public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	int total = 0;
	int clicks = 0;
	for (Text value : values){
	    total++;
	    if(value.toString().equals("1")){
		clicks++;
	    }
	}
	System.out.println(clicks);
	//System.out.println(total);
	//	System.out.println("total = " + total + " clicks= " + clicks);
	double clickrate = (double)clicks/total;
	//System.out.println(clickrate);
	context.write(key, new DoubleWritable(clickrate));
	
    }	
	
     }
}
