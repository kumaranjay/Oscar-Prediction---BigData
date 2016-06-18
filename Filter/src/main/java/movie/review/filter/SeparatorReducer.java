package movie.review.filter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SeparatorReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>{

	
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		String value = "";
		while (values.hasNext()) 
	     { 
			value = ((Text)values.next()).toString();
			output.collect(new Text(key.toString()+","), new Text(value));
	     }
	}

}
