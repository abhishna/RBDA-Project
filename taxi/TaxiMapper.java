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

public class TaxiMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String outputKey = "";
        String outputValue = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        String tripZipCode, tripMonth, tripYear, tripMiles, tripFare, tripCompany, tripPickupLat, tripPickupLong;
        String nearestZipCode = null;

        if(key.get()==0){
            // First line contains the COLUMN headers
            // Extract ZipCode, Month, Year, Company, Number of trips, Total Fare, Total Miles, Avg Fare/mile
        }
        else{

            //Trip Month
            if(columns[2].length() == 0){
                context.getCounter(TaxiCleaner.TaxiCounter.MISSING_MONTH).increment(1);
                tripMonth = "";
            }else{
                tripMonth = columns[2].substring(3,5);
            }

            //Trip Year
            if(columns[2].length() == 0){
                context.getCounter(TaxiCleaner.TaxiCounter.MISSING_YEAR).increment(1);
                tripYear = "";
            }else{
                tripYear = columns[2].substring(6,10);
            }


            //Trip Company
            if(columns[16].length() == 0){
                context.getCounter(TaxiCleaner.TaxiCounter.MISSING_COMPANY).increment(1);
                tripCompany = "";
            }else{
                tripCompany = columns[16];
            }

            //Trip Miles
            if(columns[5].length() == 0){
                context.getCounter(TaxiCleaner.TaxiCounter.MISSING_MILES).increment(1);
                tripMiles = "";
            }else{
                tripMiles = columns[5];
            }

            //Trip Fare
            if(columns[10].length() == 0){
                context.getCounter(TaxiCleaner.TaxiCounter.MISSING_FARE).increment(1);
                tripFare = "";
            }else{
                tripFare = columns[10];
            }

            //Pickup Latitude
            if(columns[17].length() == 0){
                context.getCounter(TaxiCleaner.TaxiCounter.MISSING_PICKUP_LATITUDE).increment(1);
                tripPickupLat = "";
            }else{
                tripPickupLat = columns[17];
            }

            //Pickup Longitude
            if(columns[18].length() == 0){
                context.getCounter(TaxiCleaner.TaxiCounter.MISSING_PICKUP_LONGITUDE).increment(1);
                tripPickupLong = "";
            }else{
                tripPickupLong = columns[18];
            }

            //Zipcode calculations
            if(tripPickupLat.length() != 0 && tripPickupLong.length() != 0){
                float lati = Float.parseFloat(tripPickupLat);
                float longi = Float.parseFloat(tripPickupLong); 
                nearestZipCode = ZipCodeData.findNearestZipCode(lati, longi);
            }

            if(nearestZipCode == null) {
                context.getCounter(TaxiCleaner.TaxiCounter.NO_ZIPCODE).increment(1);
            }

            //Increment Total records count
            context.getCounter(TaxiCleaner.TaxiCounter.TOTAL_RECORDS).increment(1);
            
            //Write Output
            if(tripPickupLat.length() != 0 && tripPickupLong.length() != 0){
                outputKey = nearestZipCode + "," + tripMonth + "," + tripYear + "," + tripCompany;
                if(tripFare.equals("")){
                    tripFare = "0.0";
                }
                if(tripMiles.equals("")){
                    tripMiles = "0.0";
                }
                outputValue =  "1," + tripFare + "," + tripMiles;
                context.getCounter(TaxiCleaner.TaxiCounter.VALID_RECORDS).increment(1);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        
    }
}