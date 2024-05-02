import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxiReducer
    extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
    int total_trips = 0;
    double total_fare = 0.0;
    double total_miles = 0.0;

    for (Text value : values) {
      String val = value.toString();
      String[] columns = val.split(",", -1);
      int trip = Integer.parseInt(columns[0]);
      double fare = Double.parseDouble(columns[1]);
      double miles = Double.parseDouble(columns[2]);
      total_trips += trip;
      total_fare += fare;
      total_miles += miles;
    }

    double avgFarePerMile = total_fare/(double)total_miles;
    
    String result = String.valueOf(total_trips) + "," + String.valueOf(total_fare) + "," + String.valueOf(total_miles) + "," + String.valueOf(avgFarePerMile);
    context.write(key, new Text(result));
  }
}