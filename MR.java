

import java.io.IOException;

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
	  public void reduce(Text nodeKey, Iterator<Text> neighbors, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int count=0;
	    while (neighbors.hasNext()) {
	      String neighbor= neighbors.next().toString();
	      count++;
	    }
	    output.collect(nodeKey, new Text(new Integer(count).toString()));
	  }
}
class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
  public static int row=0;
  public void map(Text node, Text neighbor,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	 if(++row>=5){
		 output.collect(node, neighbor);
	 }
  }
}
public class MR {
  public static void main(String[] args) throws IOException {
    JobConf configuration = new JobConf(MR.class);
    configuration.setJobName("MR");

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
  }
}
