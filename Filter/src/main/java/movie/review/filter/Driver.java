package movie.review.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class Driver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		JobClient client = new JobClient();
		JobConf conf = new JobConf(Driver.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(SeparatorMapper2.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		Path p1 = new Path("/Users/kumaran_jay/Documents/BigData_Final_Kumaran/ml-latest/movies");
		FileInputFormat.setInputPaths(conf, p1);
		FileOutputFormat.setOutputPath(conf, new Path("/Users/kumaran_jay/Documents/BigData_Final_Kumaran/ml-latest/movies/output"));
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setReducerClass(SeparatorReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
