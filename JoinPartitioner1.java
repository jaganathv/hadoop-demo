package Project1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner1 extends Partitioner<TextPair, IntWritable> {
	  
    @Override
    public int getPartition(TextPair key, IntWritable val, int numPartitions) 
    {
    	int hash1 = (key.getFirst().toString().hashCode()) & Integer.MAX_VALUE;
    	       		   	
        int partition = hash1 % numPartitions;
        return partition;
    }
 
}


