import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by Rajiv on 3/20/17.
 * Mapper Only Job which takes two different input paths using MultipleInputs Library
 * and writes the names of the persons who have the age field correct
 *
 * Contains two Mappers
 * a. Processes CSV file
 * b. Processes Fixed Byte file
 *
 */
public class MultiInputFilter {

    public static void main(String args[]) throws Exception {


        // Check for input Arguments count
        if (args.length != 3) {
            System.out.print("Not Enough arguments to run the code");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(MultiInputFilter.class);
        job.setJobName("Multi Input Path Mapper Application");

        // Set input paths for two mapper functions
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, CSVMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, FBMapper.class);

        // Set the Output file Path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Set the number of reducers
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
