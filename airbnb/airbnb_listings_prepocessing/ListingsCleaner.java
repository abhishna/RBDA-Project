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

public class ListingsCleaner {
    enum Counter {
        NUM_INVALID_ID,
        NUM_INVALID_IS_HOST_SINCE,
        NUM_INVALID_RESPONSE_RATE,
        NUM_INVALID_ACCEPTANCE_RATE,
        NUM_INVALID_IS_SUPERHOST,
        NUM_INVALID_TOTAL_LISTINGS,
        NUM_INVALID_HAS_PROFILE_PIC,
        NUM_INVALID_HOST_ID_VERIFIED,
        NUM_INVALID_LATITUDE,
        NUM_INVALID_LONGITUDE,
        NUM_INVALID_ACCOMODATES,
        NUM_INVALID_AVAIL_30,
        NUM_INVALID_AVAIL_60,
        NUM_INVALID_AVAIL_90,
        NUM_INVALID_AVAIL_365,
        NUM_INVALID_NUM_REVIEWS,
        NUM_INVALID_RATING
    }
 
   public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CSV Filter");
        job.setJarByClass(ListingsCleaner.class);
        job.setMapperClass(ListingsMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}