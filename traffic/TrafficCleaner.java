import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficCleaner {
    enum TrafficCounter {
        MISSING_DATE,
        MISSING_DAY, 
        MISSING_HOUR,
        MISSING_MONTH,
        MISSING_LATITUDE, 
        MISSING_LONGITUDE, 
        MISSING_WEATHER_COND,
        MISSING_INJURIES,
        NO_ZIPCODE,
        TOTAL_RECORDS, 
        VALID_RECORDS
    }
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ","); // Setting space as separator
        Job job = Job.getInstance(conf, "CSV Filter");
        
        job.setJarByClass(TrafficCleaner.class);
        job.setMapperClass(TrafficMapper.class);
        job.setCombinerClass(TrafficReducer.class);
        job.setReducerClass(TrafficReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}