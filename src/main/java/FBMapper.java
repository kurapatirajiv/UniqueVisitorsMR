import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Created by Rajiv on 3/20/17.
 */
public class FBMapper extends Mapper<LongWritable, Text, Text, Text> {

    //   Sample Input Fixed Byte file
    //     SudheerM223.78
    //     SriKantMa93.03
    //     Uday   M184.00


    // Reads the FB file and validates the age
    @Override
    public void map(LongWritable Key,Text value,Context context) throws IOException, InterruptedException{

        String regex = "^\\d+$";
        String age = value.toString().substring(8,10);
        if((age.length() == 2) && (age.matches(regex))) {
            context.write(new Text(value.toString().substring(0,7)), new Text("FB Mapper Function"));
        }

    }

}
