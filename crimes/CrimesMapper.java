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

public class CrimesFilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private HashSet<String> primaryTypes;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        primaryTypes = new HashSet<>();

    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String outputValue = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        if(key.get()==0){

            outputValue += columns[2] + "," + columns[5] + "," + columns[8] + "," + columns[17] + "," + columns[19] + "," + columns[20] + "\n";
            context.write(NullWritable.get(), new Text(outputValue));
        }
        else{
            outputValue += columns[2].substring(0,2) + "," + columns[5] + "," + columns[8] + "," + columns[17] + "," + columns[19] + "," + columns[20] + "\n";
            primaryTypes.add(columns[5]);
            if(columns[17].equals("2021")){
                Counter c = context.getCounter(CrimesCleaner.Counter.ARREST_2021);
                c.increment(1);
            }
            
            context.write(NullWritable.get(), new Text(outputValue));
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        String s="";
        for(String ptype : primaryTypes)
            s+=ptype+"\n";
        System.out.println("Primary Types : \n"+s);
    }
}