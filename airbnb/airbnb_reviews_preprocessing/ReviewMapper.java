import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.LongWritable;
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
import java.util.regex.Pattern;

public class ReviewMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    public static boolean isValidInteger(String integer) {
        String regex = "-?\\d+";
        return Pattern.matches(regex, integer);
    }

     public static String cleanString(String input) {
        // Remove punctuation using regular expression
        String cleanString = input.replaceAll("[^a-zA-Z0-9 ]", "");;

        // Convert to lower case
        cleanString = cleanString.toLowerCase();

        return cleanString;
    }
    @Override
    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
        Counter c1 = context.getCounter(ReviewCleaner.Counter.NUM_INVALID_LISTING_ID);
        Counter c2 = context.getCounter(ReviewCleaner.Counter.NUM_INVALID_ID);
        Text value = new Text(val.toString().replaceAll("\n","").replaceAll("<br/>","")); 

        String outputValue = "";
        String line = value.toString();
        String[] columns = line.split("\t");
        if(columns.length >5){
                if(key.get()==0){

                outputValue += columns[0] + "," + columns[1] + "," + columns[5];
                context.write(NullWritable.get(), new Text(outputValue));
            }
            else{
                columns[5]= cleanString(columns[5]);
                
                outputValue += columns[0] + "," + columns[1] + "," + columns[5];

               
                if(!isValidInteger(columns[0])){
                    c1.increment(1); System.out.println("invalid id "+columns[0]);
                }
                if(!isValidInteger(columns[1])){
                    c2.increment(1);
                }

                context.write(NullWritable.get(), new Text(outputValue));
            }
        }
    }
}