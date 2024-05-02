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

public class ListingsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private HashSet<String> responseTime;
    private HashSet<String> roomType;
    private HashMap<String, Integer> frequencyMap;


    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        responseTime = new HashSet<>();
        roomType = new HashSet<>();
        frequencyMap = new HashMap<>();

    }

    public boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean isValidInteger(String integer) {
        String regex = "-?\\d+";
        return Pattern.matches(regex, integer);
    }

    public boolean isDouble(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isValidDateFormat(String date) {
        // Regular expression to match yyyy-MM-dd format
        String regex = "\\d{4}-\\d{2}-\\d{2}";
        return Pattern.matches(regex, date);
    }

    public boolean isValidPercentage(String perc){
        String regex = "\\d+(\\.\\d+)?%";
        if(!Pattern.matches(regex, perc))
            return false;
        
        double value = Double.parseDouble(perc.replaceAll("%", ""));
        return value>=0.0 && value<=100.0;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter c1 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_ID);
        Counter c2 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_RESPONSE_RATE);
        Counter c3 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_ACCEPTANCE_RATE);
        Counter c4 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_IS_SUPERHOST);
        Counter c5 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_TOTAL_LISTINGS);
        Counter c6 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_HAS_PROFILE_PIC);
        Counter c7 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_HOST_ID_VERIFIED);
        Counter c8 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_LATITUDE);
        Counter c9 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_LONGITUDE);
        Counter c10 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_ACCOMODATES);
        Counter c11 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_AVAIL_30);
        Counter c12 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_AVAIL_60);
        Counter c13 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_AVAIL_90);
        Counter c14 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_AVAIL_365);
        Counter c15 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_NUM_REVIEWS);
        Counter c16 = context.getCounter(ListingsCleaner.Counter.NUM_INVALID_RATING);


        String outputValue = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if(columns.length > 61){
            if(key.get()==0){

                outputValue += columns[0] + "," + columns[12] + "," + columns[15] + "," + columns[16] + "," + columns[17] + "," + columns[18] + "," + columns[23] + "," + columns[25] + "," + columns[26] + "," + columns[28] + "," + columns[30] + "," + columns[31] +  "," + columns[33] + "," + columns[34] + "," + columns[51] + "," + columns[52] + "," + columns[53] + "," + columns[54] + "," + columns[56] + "," + columns[61];
                context.write(NullWritable.get(), new Text(outputValue));
            }
            else{
                columns[16]= columns[16].equals("N/A") ? "0":columns[16];
                columns[17]= columns[17].equals("N/A") ? "0":columns[17];
                columns[16]=columns[16].replaceAll("%","")+"%";
                columns[17]=columns[17].replaceAll("%","")+"%";
                columns[18]= columns[18].equals("") ? "f":columns[18];
                columns[61]= columns[61].equals("") ? "0":columns[61];

                outputValue += columns[0] + "," + columns[12] + "," + columns[15] + "," + columns[16] + "," + columns[17] + "," + columns[18] + "," + columns[23] + "," + columns[25] + "," + columns[26] + "," + columns[28] + "," + columns[30] + "," + columns[31] + ","  + columns[33] + "," + columns[34] + "," + columns[51] + "," + columns[52] + "," + columns[53] + "," + columns[54] + "," + columns[56] + "," + columns[61];
                responseTime.add(columns[15]);
                roomType.add(columns[33]);
                if (frequencyMap.containsKey(columns[28])) {
                    frequencyMap.put(columns[28], frequencyMap.get(columns[28]) + 1);
                }
                else {
                    frequencyMap.put(columns[28], 1);
                }
                
                if(!isValidInteger(columns[0])){
                    c1.increment(1);
                }

                if(!isValidPercentage(columns[16])){
                    c2.increment(1);
                }

                if(!isValidPercentage(columns[17])){
                    c3.increment(1);
                }

                if((!columns[18].equals("t") && !columns[18].equals("f"))){
                    c4.increment(1);
                }

                if(!isInteger(columns[23])){
                    c5.increment(1);
                }

                if((!columns[25].equals("t") && !columns[25].equals("f"))){
                    c6.increment(1);
                }

                if((!columns[26].equals("t") && !columns[26].equals("f"))){
                    c7.increment(1);
                }

                if(!isDouble(columns[30])){
                    c8.increment(1);
                }

                if(!isDouble(columns[31])){
                    c9.increment(1);
                }

                if(!isInteger(columns[34])){
                    c10.increment(1);
                }

                if(!isInteger(columns[51]) || Integer.parseInt(columns[51])<0 || Integer.parseInt(columns[51])>30 ){
                    c11.increment(1);
                }

                if(!isInteger(columns[52]) || Integer.parseInt(columns[52])<0 || Integer.parseInt(columns[52])>60 ){
                    c12.increment(1);
                }

                if(!isInteger(columns[53]) || Integer.parseInt(columns[53])<0 || Integer.parseInt(columns[53])>90 ){
                    c13.increment(1);
                }

                if(!isInteger(columns[54]) || Integer.parseInt(columns[54])<0 || Integer.parseInt(columns[54])>365 ){
                    c14.increment(1);
                }

                if(!isValidInteger(columns[56])){
                    c15.increment(1);
                }

                if(!isDouble(columns[61]) || Double.parseDouble(columns[61])<0 || Double.parseDouble(columns[61])>5 ){
                    c16.increment(1);
                }




                context.write(NullWritable.get(), new Text(outputValue));
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        String s="";
        for(String rTime : responseTime)
            s+=rTime+"\n";
        System.out.println("Response Times : \n"+s);

        s="";
        for(String rType : roomType)
            s+=rType+"\n";
        System.out.println("Room Types : \n"+s);

        System.out.println("\nNumber of Listings per Neighbourhood : ");
        System.out.println("neighbourhood,num_listings");
        for (String word : frequencyMap.keySet()) {
            System.out.println(word + "," + frequencyMap.get(word));
        }
    }
}