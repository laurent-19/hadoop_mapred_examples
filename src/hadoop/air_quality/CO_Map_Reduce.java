package hadoop.air_quality;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CO_Map_Reduce {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text stationMonth = new Text();
        private Text coValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Date,Source,Site ID")) {
                return; // Skip the header line
            }
            String[] parts = line.split(",");
            if (parts.length < 21) {
                return; // Skip this line because it does not have enough fields
            }
            String stationId = parts[2]; // Site ID
            String stationName = parts[7]; // Site Name
            String date = parts[0];
            String[] dateParts = date.split("/");
            if (dateParts.length < 3) {
                return; // Skip invalid date formats
            }
            String month = dateParts[0];
            String year = dateParts[2];
            // Pad the month with a leading zero if it's a single digit
            if (month.length() == 1) {
                month = "0" + month;
            }
            String monthYear = month + "/" + year; // Month and year
            String coConcentration = parts[4]; // Daily Max 8-hour CO Concentration
        
            stationMonth.set(stationId + "_" + stationName + "_" + monthYear);
            coValue.set(coConcentration);
            context.write(stationMonth, coValue);
        }
    }

    public static class FloatSumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(new Text("StationID_StationName_Month"), new Text("Average CO Concentration"));
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (Text val : values) {
                sum += Float.parseFloat(val.toString());
                count++;
            }
            if (count == 0) {
                return; // Skip if there were no valid values
            }
            float average = sum / count;
            result.set(Float.toString(average));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CO concentration");
        job.setJarByClass(CO_Map_Reduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(FloatSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
