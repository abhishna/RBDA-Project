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

public class CrimesCleaner {
    enum Counter {
        REPORT_JAN,
        REPORT_FEB,
        REPORT_MAR,
        REPORT_APR,
        REPORT_MAY,
        REPORT_JUN,
        REPORT_JUL,
        REPORT_AUG,
        REPORT_SEP,
        REPORT_OCT,
        REPORT_NOV,
        REPORT_DEC,
        ARREST,
        NO_ARREST,
        INVALID_YEAR,
        NO_ZIPCODE,
        MISSING_LOCATION
    }
 
   public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "CSV Filter");
        job.setJarByClass(CrimesCleaner.class);
        job.setMapperClass(CrimesMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
