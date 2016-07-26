package Project1;
import java.io.IOException;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import java.util.Set;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BX_BOOKS_Process {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "BX_BOOKS_Process");	    
	    
	    job.setJarByClass(BX_BOOKS_Process.class); 
	 //   job.setInputFormatClass(TextInputFormat.class);
	    job.setMapperClass(BX_Books_Mapper1.class);	   
	    
	  //  MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,MaxSpendMapper.class);	    
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	   job.setNumReduceTasks(0);
	  //  job.setSortComparatorClass(SortComparator.class);
	  //  job.setGroupingComparatorClass(GroupingComparator.class);
	    	       
	 //   job.setPartitionerClass(MaxSpendPartitioner.class);
	  //  job.setReducerClass(BX_Books_Reducer.class);  	    
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    // File Output Format
	    //@override
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}
    
  class BX_Books_Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>
  {
	  Multimap<Integer, Integer> multiMap = ArrayListMultimap.create();
	  int invalid_blank_count = 0, header_record=0, invalid_year_count=0;	  	  
	  
	  @Override  
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    { 
	
	  String delim = "(\";\")";
	  String nullString = null; String empty = new String();
	  
	  int Yr;
	  
  	  // Splitting the line based on the new delimiter
      String[] line = value.toString().split(delim);
      String year = line[3].trim();
       
      System.out.println("Year before validation:"+year);
     //Validating the string for BLANK VALUES
      if (year.equals(empty)||year.equals(nullString)||year.equals("")) 
      {
    	  invalid_blank_count+=1; /* ELiminating Blank Records */
      }
      else if (year.equals("Year-Of-Publication")) 
      {
    	  header_record+=1;  /* Eliminating Header Record */
      }
      else  
      {
    	  Yr = Integer.parseInt(year.toString());
    	  // Validating for Invalid Year Count from the file
    	  if (Yr > 2016 || Yr == 0)
    	  {
    		 invalid_year_count+=1;
    	  }
    	  else
    	  {
    	  System.out.println("Year:"+Yr+"ONE:"+"1");
          multiMap.put(Yr,1);
    	  }
      }
   }    
	  public void cleanup(Context context) throws IOException, InterruptedException
	  {
		  //Getting all sets of Keys
		  String OUTPUT_HEADER = "YEAR\t\tNUMBER OF BOOKS PUBLISHED"; 
		  context.write(new Text(OUTPUT_HEADER),new IntWritable(0));
		  
		  Set<Integer> keys = multiMap.keySet();
		  System.out.println("Keys:"+keys);
		  SortedMap <Integer, Integer> sortedMap = new TreeMap<Integer, Integer>();	 
		  
		  
		// For each year computing the number of books using local aggregation approach
		  for (Integer key : keys) 
		  {
	            System.out.println("Key = " + key);
	               int sum=0;
	               Iterator<Integer>  c = multiMap.get(key).iterator();
	               while(c.hasNext())
	               {
	            	   sum+=c.next();        
	            	   
	               }
	               // Adding Year and the total Number of books published into a sortedMap
	               sortedMap.put(sum,key);
	               System.out.println("key:"+key+" "+"Value:"+sum);	          	
	               context.write(new Text(key.toString()),new IntWritable(sum));            	
	                        
	     }
		   // Command to retrieve the year which had more number of books published
		  String Year_With_Max_Publication = sortedMap.get(sortedMap.lastKey())+"\t:"+ sortedMap.lastKey()+"\t\t\t\t\t\t\t\t\t\t\t\t\t";		    
		    
		  context.write(new Text("\n YEAR WITH MAXIMUM PUBLICATIONS"), new IntWritable(0));
		  context.write(new Text(Year_With_Max_Publication),new IntWritable(0));
		   
		  context.write(new Text("\nInvalid_Year_Count:"),new IntWritable(invalid_year_count));
		  context.write(new Text("\nInvalid_blank_Count:"),new IntWritable(invalid_blank_count));
	  }
  }
 
 
    