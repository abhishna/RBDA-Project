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

import java.util.HashSet;

public class TrafficMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String outputKey = "";
        String outputValue = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        String crashDate, crashYear, crashHr, crashDay, crashMonth, crashLat, crashLong, weather, injuries;
        String nearestZipCode = null;

        if(key.get()==0){
            // First line contains the COLUMN headers Skip this line
            // Extract ZipCode, Month, Year, Number of crash, Number of injuries
        }
        else{
            if(columns[2].length() == 0){
                context.getCounter(TrafficCleaner.TrafficCounter.MISSING_DATE).increment(1);
                crashDate = "";
                crashYear = "";
            }else{
                crashDate = columns[2];
                crashYear = columns[2].substring(6,10);
            }

            if(columns[44].length() == 0){
                context.getCounter(TrafficCleaner.TrafficCounter.MISSING_MONTH).increment(1);
                crashMonth = "";
            }else{
                crashMonth = columns[44];
                if(crashMonth.length() == 1){
                    crashMonth = "0" + crashMonth;
                }
            }

            if(columns[45].length() == 0){
                context.getCounter(TrafficCleaner.TrafficCounter.MISSING_LATITUDE).increment(1);
                crashLat = "";
            }else{
                crashLat = columns[45];
            }

            if(columns[46].length() == 0){
                context.getCounter(TrafficCleaner.TrafficCounter.MISSING_LONGITUDE).increment(1);
                crashLong = "";
            }else{
                crashLong = columns[46];
            }

            if(columns[6].length() == 0){
                context.getCounter(TrafficCleaner.TrafficCounter.MISSING_WEATHER_COND).increment(1);
                weather = "";
            }else{
                weather = columns[6];
                context.getCounter("CRASH_WEATHER", weather).increment(1);
            }

            if(columns[35].length() == 0){
                context.getCounter(TrafficCleaner.TrafficCounter.MISSING_INJURIES).increment(1);
                injuries = "0";
            }else{
                injuries = columns[35];
            }

            //Zipcode calculations
            if(crashLat.length() != 0 && crashLong.length() != 0){
                float lati = Float.parseFloat(crashLat);
                float longi = Float.parseFloat(crashLong); 
                nearestZipCode = ZipCodeData.findNearestZipCode(lati, longi);
            }

            if(nearestZipCode == null) {
                context.getCounter(TrafficCleaner.TrafficCounter.NO_ZIPCODE).increment(1);
            }

            // dynamic counter
            context.getCounter("CRASH_YEAR", crashYear).increment(1);
            
            //Increment Total records count
            context.getCounter(TrafficCleaner.TrafficCounter.TOTAL_RECORDS).increment(1);
            
            //Write Output
            if(nearestZipCode != null){
                outputKey += nearestZipCode + "," + crashMonth + "," + crashYear;
                outputValue += "1," + injuries;
                context.getCounter(TrafficCleaner.TrafficCounter.VALID_RECORDS).increment(1);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        
    }
}