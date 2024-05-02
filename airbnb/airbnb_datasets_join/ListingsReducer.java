import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
public class ListingsReducer
    extends Reducer<Text, Text, NullWritable, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
    ArrayList<String> l = new ArrayList<>();
    String b="";
    for (Text value : values) {
      String v = value.toString();
      if(v.charAt(0)=='A'){
          l.add(v.substring(1));
      }
      else{
        b = v.substring(1);
      }
    }
    for(String v : l){
      if(!b.equals(""))
      context.write(NullWritable.get(),new Text(key+","+v+","+b));
    }
  }
}