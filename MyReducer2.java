//package wiki.hadoopcomposite;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducer2 extends
		Reducer<CompositeKey, Text, Text, Text> {

	/**
	 * Reducer do the actual matrix multiplication.
	 * 
	 * @param key
	 *            is the cell unique cell dimension (00) represents cell 0,0
	 * @value values required to calculate matrix multiplication result of that
	 *        cell.
	 */

	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		// System.out.println("Dimension from Reducer = " + dimension);
		try {
			int count = 0;
			String timeS = "";
			ArrayList<ArrayList<Text>> editSessions = new ArrayList<ArrayList<Text>>();
			ArrayList<Text> session = new ArrayList<Text>();
			SimpleDateFormat sdf = new SimpleDateFormat(
					"yyyy-MM-dd'T'HH:mm:ss'Z'");
			sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			Date oldDate = null, newDate;
			int i = 0;
			String times="";
			for (Text val : values) {
				count++;
				times+=val.toString()+" ";
			}
			

			
			
			context.write(key.getUser(), new Text(count+" : "+times));

		} catch (Exception e) {
		}
	}

}
