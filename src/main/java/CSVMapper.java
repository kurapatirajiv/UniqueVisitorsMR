import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Rajiv on 3/20/17.
 */
public class CSVMapper extends Mapper<LongWritable, Text, Text, Text> {

    // Sample Input CSV file
    //     Rajiv,M,28,3.78
    //     Abe,F,999,3.78
    //     Lucas,M,12,4.0

    // Reads the CSV file and validates the age
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Process the CSV file
        String[] input = value.toString().split(",");
        String regex = "^\\d+$";
        String age = input[2];
        if((age.length() == 2) && (age.matches(regex))) {
                context.write(new Text(input[0]), new Text("CSV Mapper Function"));
           }
        }
    }
