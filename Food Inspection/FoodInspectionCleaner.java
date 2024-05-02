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

public class FoodInspectionCleaner {
    enum FoodInspectorCounter {
        MISSING_NAME,
	MISSING_ID,
        MISSING_FACILITY_TYPE, 
        MISSING_RISK,
        MISSING_CITY, 
        MISSING_ZIP,
        MISSING_RESULTS, 
        MISSING_LATITUDE,
        MISSING_LONGITUDE,
        TOTAL_RECORDS, 
        VALID_RECORDS
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CSV Filter");
        
        job.setJarByClass(FoodInspectionCleaner.class);
        job.setMapperClass(FoodInspectionMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
