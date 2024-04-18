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

public class CrimesMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private HashSet<String> primaryTypes;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        primaryTypes = new HashSet<>();
        

    }


    // Key - month (2), year (17), primary_type (5)
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String outputValue = "";
        String keyValue = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        Counter jan = context.getCounter(CrimesCleaner.Counter.REPORT_JAN);
        Counter feb = context.getCounter(CrimesCleaner.Counter.REPORT_FEB);
        Counter mar = context.getCounter(CrimesCleaner.Counter.REPORT_MAR);
        Counter apr = context.getCounter(CrimesCleaner.Counter.REPORT_APR);
        Counter may = context.getCounter(CrimesCleaner.Counter.REPORT_MAY);
        Counter jun = context.getCounter(CrimesCleaner.Counter.REPORT_JUN);
        Counter jul = context.getCounter(CrimesCleaner.Counter.REPORT_JUL);
        Counter aug = context.getCounter(CrimesCleaner.Counter.REPORT_AUG);
        Counter sep = context.getCounter(CrimesCleaner.Counter.REPORT_SEP);
        Counter oct = context.getCounter(CrimesCleaner.Counter.REPORT_OCT);
        Counter nov = context.getCounter(CrimesCleaner.Counter.REPORT_NOV);
        Counter dec = context.getCounter(CrimesCleaner.Counter.REPORT_DEC);

        Counter arrest = context.getCounter(CrimesCleaner.Counter.ARREST);
        Counter no_arrest = context.getCounter(CrimesCleaner.Counter.NO_ARREST);

        Counter invalid_year = context.getCounter(CrimesCleaner.Counter.INVALID_YEAR);

        


        if(key.get()==0){

            outputValue += columns[2] + "," + columns[5] + "," + columns[8] + "," + columns[17] + "," + columns[19] + "," + columns[20] + "\n";
            context.write(NullWritable.get(), new Text(outputValue));
        }
        else{
            
            outputValue += columns[2].substring(0,2) + "," + columns[5] + "," + columns[8] + "," + columns[17] + "," + columns[19] + "," + columns[20] + "\n";

            primaryTypes.add(columns[5]);
            if(columns[2].substring(0,2).equals("01")){
                jan.increment(1);
            }
            else if(columns[2].substring(0,2).equals("02")){
                feb.increment(1);
            }
            else if(columns[2].substring(0,2).equals("03")){
                mar.increment(1);
            }
            else if(columns[2].substring(0,2).equals("04")){
                apr.increment(1);
            }
            else if(columns[2].substring(0,2).equals("05")){
                may.increment(1);
            }
            else if(columns[2].substring(0,2).equals("06")){
                jun.increment(1);
            }
            else if(columns[2].substring(0,2).equals("07")){
                jul.increment(1);
            }
            else if(columns[2].substring(0,2).equals("08")){
                aug.increment(1);
            }
            else if(columns[2].substring(0,2).equals("09")){
                sep.increment(1);
            }
            else if(columns[2].substring(0,2).equals("10")){
                oct.increment(1);
            }
            else if(columns[2].substring(0,2).equals("11")){
                nov.increment(1);
            }
            else if(columns[2].substring(0,2).equals("12")){
                dec.increment(1);
            }
    
            if(columns[8].equals("true")) {
                arrest.increment(1);
            }
            else if(columns[8].equals("false")) {
                no_arrest.increment(1);
            }
            
            int int_year = Integer.parseInt(columns[17]);
            if(int_year < 2001 || int_year > 2024) {
                invalid_year.increment(1);
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
