package Project1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import Project1.GroupingComparator1;
import Project1.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Rank_Books_Published_in_2002
{
	public static void main(String[] args) throws Exception
	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Rank_Books_Published_in_2002");
	        job.setJarByClass(Rank_Books_Published_in_2002.class); 
	 	    job.setInputFormatClass(TextInputFormat.class);
	 	    
	 	    MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,BX_Book_Mapper.class);
		    MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,BX_Book_Ratings_Mapper.class);
		    
		    job.setMapOutputKeyClass(TextPair.class);
		    job.setMapOutputValueClass(IntWritable.class); 	    
	 	    
	 	    
	 	    job.setSortComparatorClass(SortComparator.class);
		    job.setGroupingComparatorClass(GroupingComparator1.class);
		    job.setPartitionerClass(JoinPartitioner1.class);
		    job.setNumReduceTasks(1);
	 	    job.setReducerClass(BX_Book_Ratings_Reducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);																
		    
		    // File Output Format
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));    
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);																																				
	 	     	    	 	    
	}
}

class BX_Book_Mapper extends Mapper<LongWritable, Text, TextPair, IntWritable>
{
	int invalid_blank_count_m1 = 0, header_record_m1=0, invalid_year_count_m1=0;
	/*public void setup(Context context) throws IOException, InterruptedException
	{
		
		context.write(new TextPair(new Text("PRINTING OUTPUT FROM MAPPER_A"), new Text("")), new IntWritable(0));
	}*/
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	  String delim1 = "(\";\")"; 
		
	  
	  int Yr;
	  
  	  // Splitting the line //based on the new delimiter
      String[] line1 = value.toString().split(delim1);
      String year = trimQuotes1(line1[3].trim());
	  String ISBN = trimQuotes1(line1[0].trim());       
      
      if(!year.equals("Year-Of-Publication") && !ISBN.equals("ISBN") && !year.equals("") && !ISBN.equals("")) 
      {
    	  Yr = Integer.parseInt(year.toString());
    	  
    	  //Checking for the year 2002
    	  if(Yr == 2002)
    	  {
			 System.out.println("ISBN_Cd_from_Mapper_A:"+ISBN);
    		 context.write(new TextPair(new Text(ISBN),new Text("a")),new IntWritable(1));
    	  }    	 
      }    
  }
	  
	  public static String trimQuotes1 (String value)
	  {
		  if(value == null)
			return value;

          if(value.startsWith("\""))
		  {
			  value=value.substring(1,value.length());
		  }    			  
		  if(value.endsWith("\""))
		  {
			  value=value.substring(0,value.length()-1);
		  }		
		return value;		  
	  }
}

class BX_Book_Ratings_Mapper extends Mapper<LongWritable, Text, TextPair, IntWritable>
{
	
	@Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	   	  // Splitting the line based on the new delimiter
	  String[] line2 = value.toString().split(";");
      String ISBN_Rating = trimQuotes(line2[1].trim()); 
	  String Rating = trimQuotes(line2[2].trim());  
       
      
     if (!ISBN_Rating.equals("ISBN") && !Rating.equals("Book-Rating")) 
      {    	  
    	
    	  int RTNG_CD = Integer.parseInt(Rating);
    	  System.out.println("output from b mapper:"+ISBN_Rating+"\t"+RTNG_CD);
    	  context.write(new TextPair(new Text(ISBN_Rating), new Text("b")), new IntWritable(RTNG_CD));
      }    
   
  }
	
	public static String trimQuotes( String value )
	  {
	    if ( value == null )
	      return value;

	    value = value.trim( );
	    if ( value.startsWith( "\"" ) && value.endsWith( "\"" ) )
	      return value.substring( 1, value.length( ) - 1 );
	    
	    return value;
	  }
	  
	  
}

class BX_Book_Ratings_Reducer extends Reducer<TextPair,IntWritable,Text,IntWritable> 
{

    private Multimap<Integer, String> multiMap = ArrayListMultimap.create();
    boolean key_from_book_file = false;
    String ISBN_CD ="";
    private int call=0;
	public void reduce(TextPair key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
    {          
	   System.out.println("Key from reducer:"+ key);
		System.out.println("call to reducer:"+ ++call);
       ISBN_CD ="";
       key_from_book_file = false;
       
       for (IntWritable val:values)
       {    
    	   System.out.println("Value From Reducer for Key"+key+" is:"+val);
    	   if (key.getSecond().toString().equals("a"))
    	   {    		   
    		 ISBN_CD = key.getFirst().toString();
    		 System.out.println("ISBN_CD from Reducer:"+ ISBN_CD);
    		 key_from_book_file = true;
    	   }
    	   
    	   if (key.getSecond().toString().equals("b") && key_from_book_file)
    	   {
    	     int RTNG_CODE = val.get();
    	     //ISBN_CD=key.getFirst().toString();
    	     System.out.println("Rating_Code:"+ RTNG_CODE);
			 multiMap.put(RTNG_CODE, ISBN_CD);
    	   }
    	   
    	}   
      
     }
	   
	public void cleanup(Context context) throws IOException, InterruptedException
     {       
		 Set<Integer> keys = multiMap.keySet();   
		
		//	System.out.println("task #:"+ ++task);
		for (Integer key : keys) 
		  {
	            System.out.println("Key = " + key);
	               int sum=0;
	               //Collection <string> fruits = multiMap.get(key);
	               Iterator<String> c = multiMap.get(key).iterator();
	               while(c.hasNext())
	               {
	            	   c.next();
	            	   sum+=1;        
	            	   
	               }
	               // Adding Year and the total Number of books published into a sortedMap	             
	               System.out.println("key:"+key+" "+"Value:"+sum);	          	
	               context.write(new Text(key.toString()),new IntWritable(sum));            	
	                        
	     }	
		
		

	      
      }
 }

