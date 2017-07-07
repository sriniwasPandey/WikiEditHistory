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

public class MyReducer extends
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
			for (Text val : values) {

				if (i == 0) {
					oldDate = sdf.parse(val.toString());
					session.add(val);
					i++;
				} else {
					count += 1;
					newDate = sdf.parse(val.toString());
					long diff = newDate.getTime() - oldDate.getTime();
					if (diff > 3600000) {
						editSessions.add(session);
						session = new ArrayList<Text>();
					}
					session.add(val);
					oldDate = newDate;

					timeS = timeS + " : " + val;
				}
				editSessions.add(session);
			}
			int editcount = 0;
			long avg_edit_duration = 0;
			for (i = 0; i < editSessions.size(); i++) {
				ArrayList<Text> currentSession = editSessions.get(i);
				for (int j = 1; j < currentSession.size(); j++) {
					editcount++;
					avg_edit_duration += sdf.parse(
							currentSession.get(j).toString()).getTime()
							- sdf.parse(currentSession.get(j).toString())
									.getTime();
				}

			}
			avg_edit_duration = avg_edit_duration / editcount;
			long total_time=0;
			for (i = 0; i < editSessions.size(); i++) {
				ArrayList<Text> currentSession = editSessions.get(i);
				total_time=sdf.parse(
						currentSession.get(currentSession.size()-1).toString()).getTime()
						- sdf.parse(currentSession.get(0).toString())
								.getTime()+avg_edit_duration;
			}
			// 2016-04-14T07:13:02Z
			// System.out.println("reducer: "+"count :"+count+key.getUser().toString()
			// + "-" + timeS );
			context.write(key.getUser(), new Text(total_time+" : "+editcount));

		} catch (Exception e) {
		}
	}

}
