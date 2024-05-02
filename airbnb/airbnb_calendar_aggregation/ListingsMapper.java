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
import java.util.ArrayList;
import java.util.Map;

public class CalendarMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private HashMap<String, ArrayList<Double>> priceMap;


    @Override
    public void setup(Context context) throws IOException, InterruptedException{

        priceMap = new HashMap<>();

    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       
        String outputValue = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if(columns.length == 7){
            if(key.get()==0){

                outputValue += columns[0] + "," + "month,year,price";
                context.write(NullWritable.get(), new Text(outputValue));
            }
            else{

                columns[1] = columns[1].replaceAll("-","/");
                String k  = columns[0]+','+columns[1].split("/")[1]+','+columns[1].split("/")[0];
                if (priceMap.containsKey(k)) {
                    if(columns[3].indexOf(',')==-1)
                        priceMap.get(k).add(Double.parseDouble(columns[3].substring(1,columns[3].length()).replaceAll(",","")));
                    else
                        priceMap.get(k).add(Double.parseDouble(columns[3].substring(2,columns[3].length()-1).replaceAll(",","")));
                }
                else {
                    ArrayList<Double> l = new ArrayList<>();
                    if(columns[3].indexOf(',')==-1)
                        l.add(Double.parseDouble(columns[3].substring(1,columns[3].length()).replaceAll(",","")));
                    else
                        l.add(Double.parseDouble(columns[3].substring(2,columns[3].length()-1).replaceAll(",","")));
                    priceMap.put(k, l);
                }
                
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        for (Map.Entry<String, ArrayList<Double>> entry : priceMap.entrySet()) {
            String k = entry.getKey();

            ArrayList<Double> l = entry.getValue();
            double s = 0;
            for(int i=0;i<l.size();i++){
                s+=l.get(i);
            }
            double avg = s/l.size();
            context.write(NullWritable.get(), new Text(k+","+String.valueOf(avg)));
        }
    }
}