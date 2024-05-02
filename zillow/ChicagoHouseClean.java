import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChicagoHouseClean {

    enum Counter {
        ROW_COUNT,
        COLUMN_COUNT,
        CHICAGO_ZIPCODES,
        NULL_VALUES,
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ChicagoHouseClean <inputPath> <outputPath>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Filter Job");
        
        job.setJarByClass(ChicagoHouseClean.class);
        
        // Set Mapper class and output key-value types
        job.setMapperClass(ChicagoHouseCleanMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set number of reduce tasks to 0
        job.setNumReduceTasks(0);
        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // Wait for the job to complete and exit with status accordingly
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
