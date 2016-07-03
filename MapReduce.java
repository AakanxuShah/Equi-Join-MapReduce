package DDS.EquiJoin;

/**
 *Author
 *Aakanxu Shah
 *
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.*;

public class App
{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	     
		private Text kjoin = new Text();
	    private Text tuples = new Text();

	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	//Read the  line and Split 
	    	// gets the relation name, joining key and tuple
	    	String line[] = value.toString().split(",");
	    	int len = line.length;
	    	
	    	String relation = line[0];
	        //The tuple added with its relation name
	    	String tuple = relation;
	    	String keyjoins = line[1];
	       
	    	for (int i=1;i<len;i++)
	    	{
	    		tuple = tuple + ","+ line[i];	           
	    	}
	       
	    	//sets key and value
	    	kjoin.set(keyjoins);
	    	tuples.set(tuple);
	        	      
	    	output.collect(kjoin, tuples);
	    	//context.write(kjoin,tuples);
	     }
	   }

	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		   public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
	           
			   List<String>  RL = new ArrayList<String>();
			   List<String>  SL = new ArrayList<String>();
			   
			   Text result = new Text();
			   String jtuple = "";
			   
			   	/*
			   	 * 
			   	 for(Text: val: values)
			   	 {
			   	 jtuples = jtuples + val.ToString()+",";
			   	 }
			   	 
			   	 context.write();
			  */
			   while(values.hasNext())
			   { 
				   String temp = values.next().toString();
				   String Split[] = temp.split(",");
	             //checks for the joining key and add it separately in a table
				   if(Split[0].equals("S")){
					   SL.add(temp);
				   }
				   else if(Split[0].equals("R")){
					   RL.add(temp);
				   }
			   }	        	       	       	        	        	     
	        
			   //Clears the key-value if doesnt match
			   if(SL.size() == 0 || RL.size() ==0){
				   key.clear();
			   }else{
				   //Adds value of S to R
				   for(int i=0; i<RL.size(); i++){
					   for(int j=0;j<SL.size();j++){ jtuple= RL.get(i) + "," + SL.get(j);
						   result.set(jtuple);
						   output.collect(new Text(""), result);	        		
					   }
				   }
			   }	         	         	       	        	        	         	        
		   }
	   }

	   public static void main(String[] args) throws Exception {	       	 
	       
		   JobConf conf = new JobConf(App.class);
		   conf.setJobName("EquiJoin");

		   conf.setOutputKeyClass(Text.class);
		   conf.setOutputValueClass(Text.class);
		   conf.set("mapred.textoutputformat.separator"," ");
		   conf.setMapperClass(Map.class);
		   conf.setReducerClass(Reduce.class);
		   
		   
		  // Path input_path = new Path("hdfs://10.0.2.15:54310/inputfile.txt");
		   //Path output_path = new Path("hdfs://10.0.2.15:54310/outputfileAakanxu6.txt");
	    
		   FileInputFormat.setInputPaths(conf,new Path(args[0]));	     
		   FileOutputFormat.setOutputPath(conf,new Path(args[1]));

		   JobClient.runJob(conf);
	   }
}

/* Do Not Plagarize. If this code is used , please give the full credit. */
