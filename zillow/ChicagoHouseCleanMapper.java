import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import java.util.ArrayList;
import java.lang.Math;

public class ChicagoHouseCleanMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    ArrayList<String> badValues;

    public void setup(Context context) throws IOException, InterruptedException{
        badValues = new ArrayList<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {        


        String outputValue = "";
        String outputKey = "";
        String line = value.toString();
        String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        // Processing the header column
        // Counting the total number of columns and removing redundant columns
        if(key.get()==0){

            // Counter col_count = context.getCounter(ChicagoHouseClean.Counter.COLUMN_COUNT);
            // outputValue += columns[2];

            // for(int i=0; i<columns.length; i++) {
            //     col_count.increment(1);
            //     if(i >= 9) {
            //         String new_date = columns[i].substring(0, columns[i].length()-2);
            //         outputValue += new_date;
            //         outputValue += ",";
            //     }
            // }

            // outputValue = outputValue.substring(0, outputValue.length() - 1);
            // outputValue += "\n";
            // context.write(NullWritable.get(), new Text(outputValue));

            // For this, I am not going to add column headers, that can be done afterwards
            Counter col_count = context.getCounter(ChicagoHouseClean.Counter.COLUMN_COUNT); 
            col_count.increment(columns.length);
            
        }

        // Process all other rows
        else {
            String city = columns[6];
            Counter row_count = context.getCounter(ChicagoHouseClean.Counter.ROW_COUNT);
            Counter chic_count = context.getCounter(ChicagoHouseClean.Counter.CHICAGO_ZIPCODES);
            Counter incorrect_numbers = context.getCounter(ChicagoHouseClean.Counter.NULL_VALUES);
            row_count.increment(1);

            // Check if the city is "Chicago"
            // If it is, write new record for every month in the dataset
            if (city.equals("Chicago")) {
                chic_count.increment(1);

                // Housing dataset
                int col_index = 9;
                if(columns.length > 150) {
                    for(int year = 2000; year <= 2024; year++) {
                        for(int month = 1; month <= 12; month++) {
                            if(col_index < columns.length) {
                                outputKey += columns[2] + "," + String.format("%02d", month) + "," + year;
                                outputValue += columns[col_index];
                                try {
                                    float floatValue = Float.parseFloat(columns[col_index]);
                                    context.write(new Text(outputKey), new Text(outputValue));
                                } catch (NumberFormatException e) {
                                    incorrect_numbers.increment(1);
                                }
                                col_index += 1;
                                outputKey = "";
                                outputValue = "";
                            }
                        }
                    }
                }

                else {
                    for(int year = 2015; year <= 2024; year++) {
                        for(int month = 1; month <= 12; month++) {
                            if(col_index < columns.length) {
                                outputKey += columns[2] + "," + String.format("%02d", month) + "," + year;
                                outputValue += columns[col_index];
                                try {
                                    float floatValue = Float.parseFloat(columns[col_index]);
                                    context.write(new Text(outputKey), new Text(outputValue));
                                } catch (NumberFormatException e) {
                                    incorrect_numbers.increment(1);
                                }
                                col_index += 1;
                                outputKey = "";
                                outputValue = "";
                            }
                        }
                    }
                }



                // for(int i=9; i<columns.length; i++) {
                //     outputValue += columns[i];
                //     outputValue += ",";

                //     try {
                //         float floatValue = Float.parseFloat(columns[i]);
                //     } catch (NumberFormatException e) {
                //         incorrect_numbers.increment(1);
                //         badValues.add(columns[i]);
                //     }
                // }

                // outputValue = outputValue.substring(0, outputValue.length() - 1);
                // outputValue += "\n";
            }
        }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        
        System.out.println("Some of the out of format values: ");
        for(int i=0; i<Math.min(badValues.size(), 15); i++) {
            System.out.println(badValues.get(i));
        }
    }
}
