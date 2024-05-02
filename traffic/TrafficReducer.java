import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficReducer
    extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
    int total_crash = 0;
    int total_injuries = 0;

    for (Text value : values) {
      String val = value.toString();
      String[] columns = val.split(",", -1);
      int crash = Integer.parseInt(columns[0]);
      int injuries = Integer.parseInt(columns[1]);
      total_crash += crash;
      total_injuries += injuries;
    }
    
    String result = String.valueOf(total_crash) + "," + String.valueOf(total_injuries);
    context.write(key, new Text(result));
  }
}