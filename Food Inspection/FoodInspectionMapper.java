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

public class FoodInspectionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String outputValue = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        String facilityName,facilityId, facilityType, facilityRisk, facilityCity, facilityZIP, facilityResult, facilityLatitude, facilityLongitude,facilityNewCity;

        if(key.get()==0){
            // First line contains the COLUMN headers
            outputValue += columns[1] + "," +columns[3]+","+  columns[4] + "," + columns[5] + "," + columns[7] + "," + columns[9] + "," + columns[12] + "\n";
            context.write(NullWritable.get(), new Text(outputValue));
        }
        else{
            if(columns[1].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_NAME).increment(1);
                facilityName = "";
            }else{

                facilityName = columns[1].replaceAll(",", "");
            }
            if(columns[3].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_ID).increment(1);
                facilityId = "";
            }else{
                facilityId = columns[3].replaceAll(",", "");
            }
            if(columns[4].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_FACILITY_TYPE).increment(1);
                facilityType = "";
            }else{
                facilityType = columns[4].replaceAll(",", "");
               // context.getCounter("facility_Types", facilityType).increment(1);
            }

            if(columns[5].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_RISK).increment(1);
                facilityRisk = "";
            }else{
                facilityRisk = columns[5];
                context.getCounter("facility_Risks", facilityRisk).increment(1);
            }

            if(columns[7].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_CITY).increment(1);
                facilityCity = "";
            }
            else if(!columns[7].toLowerCase().contains("chicago".toLowerCase()))
            {
                
                facilityNewCity = columns[7];
                //context.getCounter("Other_Cities", facilityNewCity).increment(1);
                facilityCity = "";
            }
            else
            {
                facilityCity = "Chicago";
            }

            if(columns[9].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_ZIP).increment(1);
                facilityZIP = "";
            }else{
                facilityZIP = columns[9];
            }

            if(columns[12].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_RESULTS).increment(1);
                facilityResult = "";
            }else{
                facilityResult = columns[12];
                context.getCounter("Different_Results", facilityResult).increment(1);
            }

            if(columns[14].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_LATITUDE).increment(1);
                facilityLatitude = "";
            }else{
                facilityLatitude = columns[14];
            }

            if(columns[15].length() == 0){
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.MISSING_LONGITUDE).increment(1);
                facilityLongitude = "";
            }else{
                facilityLongitude = columns[15];
            }

            outputValue += facilityName + "," +facilityId+","+ facilityType + "," + facilityRisk + "," + facilityCity + "," + facilityZIP + "," + facilityResult;
            // dynamic counter
            
            //Increment Total records count
            context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.TOTAL_RECORDS).increment(1);
            
            //Write Output
            if(facilityCity.equalsIgnoreCase("chicago") && facilityZIP.length() != 0 && facilityId.length()!=0 && facilityName.length()!=0&&facilityResult.length()!=0&&facilityType.length()!=0&&facilityRisk.length()!=0)
{
                context.getCounter(FoodInspectionCleaner.FoodInspectorCounter.VALID_RECORDS).increment(1);
                context.write(NullWritable.get(), new Text(outputValue));
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        
    }
}
