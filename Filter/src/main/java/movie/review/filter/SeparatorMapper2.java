package movie.review.filter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SeparatorMapper2 extends MapReduceBase
		implements
			Mapper<LongWritable, Text, IntWritable, Text> {

	String out = "";

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
					throws IOException {
		String values[] = parseInput(value.toString());
		if (values.equals(null))
			return;
		String keyIn = "";
		String valueIn = "";

		if (values[2].equals(""))
			return;
		keyIn = values[0];
		valueIn = values[1] + "," + values[2] + "," + values[3];
		System.out.println(keyIn + " " + valueIn);
		output.collect(new IntWritable(Integer.parseInt(keyIn)),
				new Text(valueIn));

	}

	public String[] parseInput(String input) {
		String output[] = new String[4];
		String values[] = input.split(",");
		String genres = "";
		if (input.contains("\"") && values.length >= 4) {
			String movieId = values[0];
			genres = values[values.length - 1];
			output[0] = movieId;
			output[3] = genres;
			int openQuotes = input.indexOf('"');
			int closeQuotes = input.indexOf('"', openQuotes + 1);
			String betweenQuotes = input.substring(openQuotes + 1, closeQuotes);
			String temp[] = betweenQuotes.split(",");
			String year = checkYear(temp[1]);
			String movie = temp[0];
			output[1] = movie;
			output[2] = year;

		} else {
			String movieId = values[0];
			genres = values[2];
			output[0] = movieId;
			output[3] = genres;
			int bracket = values[1].indexOf('(');
			String year = "";
			String movie = values[1];
			if (bracket != -1 && bracket != 0) {
				movie = values[1].substring(0, bracket - 1);
				String yearTemp = values[1].substring(bracket);
				year = checkYear(yearTemp);
			}
			output[1] = movie;
			output[2] = year;
		}
		return output;
	}
	public String checkYear(String temp) {
		boolean flag = false;
		String yearStr = "";
		while (temp.contains("(") && temp.contains(")")) {
			int start = temp.indexOf('(');
			yearStr = temp.substring(start + 1, start + 5);
			if (start + 5 < temp.length())
				temp = temp.substring(start + 5);
			else
				temp = "";
			try {
				int year = Integer.parseInt(yearStr);
				return yearStr;
			} catch (NumberFormatException e) {
				yearStr = "";
				// continue;
			}
		}
		return yearStr;
	}

}
