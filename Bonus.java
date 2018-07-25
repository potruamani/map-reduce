
import java.io.IOException;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	  public void reduce(Text nodeKey, Iterator<Text> neighbors,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		  try{
			  Text myKey=null;int max=0,current;
		    while(neighbors.hasNext()) {
			      String neighbor= neighbors.next().toString();
			      if(!neighbor.isEmpty()){
				      current=Integer.parseInt(neighbor);
			    	  myKey=nodeKey;
			    	  if(max<current){
				       max=current;
				      }
			      }
			}
		    output.collect(myKey, new Text(new Integer(max).toString()));
	    }catch(Exception e){
		  e.printStackTrace();
	    }
	  }
}
class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	public static int row=0,nodeCount=0,prevValue=0,currentValue=0;
	  public void map(Text node, Text neighbor,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		 try{
			 if(++nodeCount>=5){
				 if(nodeCount==5){
					 currentValue=Integer.parseInt(node.toString());
					 row++;
					 output.collect(node, neighbor);
					 prevValue=currentValue;
				 }else{
					 currentValue=Integer.parseInt(node.toString());
					 if(prevValue != currentValue){
						 if(++row<=30){
							 output.collect(node, neighbor);
							 prevValue=currentValue;
						 }
					 }else{
						 if(row<=30){
							 output.collect(node, neighbor);
							 prevValue=currentValue;
						 } 
					 }
				 }
			 }
		 }catch(Exception e){
			 e.printStackTrace();
		 }
	  }
}
public class Bonus {
  public static void main(String[] args) throws IOException{
	try{
	    JobConf configuration = new JobConf(Bonus.class);
	    configuration.setJobName("Bonus");
	
	    configuration.setMapOutputKeyClass(Text.class);
	    configuration.setMapOutputValueClass(Text.class);
	
	    configuration.setOutputKeyClass(Text.class);
	    configuration.setOutputValueClass(Text.class);
	
	    configuration.setMapperClass(Map.class);
	    configuration.setReducerClass(Reduce.class);
	
	    configuration.setInputFormat(KeyValueTextInputFormat.class);
	    configuration.setOutputFormat(TextOutputFormat.class);
	
	    FileInputFormat.setInputPaths(configuration, new Path(args[0]));
	    FileOutputFormat.setOutputPath(configuration, new Path(args[1]));
	
	    JobClient.runJob(configuration);
	}catch(Exception e){
	  e.printStackTrace();
	}
  }
}
