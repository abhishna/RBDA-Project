import org.apache.hadoop.io.NullWritable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;

public class ListingsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       
        String outputValue = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        String file = context.getInputSplit().toString(); 

        if(file.contains("avg_prices")){

            if(key.get()!=0){
                context.write(new Text(columns[0]), new Text("A"+line.substring(line.indexOf(",")+1)));
            }
        }
        else{
            if(key.get()!=0){
                float lati = Float.parseFloat(columns[10]);
                float longi = Float.parseFloat(columns[11]);
                String nearestZipCode = ZipCodeData.findNearestZipCode(lati, longi);


                context.write(new Text(columns[0]),new Text( "B"+line.substring(line.indexOf(",")+1)+ "," + nearestZipCode ));
            }
        }
    }
}