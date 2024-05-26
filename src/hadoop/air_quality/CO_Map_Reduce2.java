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
import java.util.ArrayList;
import java.util.List;

public class CO_Map_Reduce2 {

    // First Mapper
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

    // First Reducer
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

    // Second Mapper
    public static class AvgMapper extends Mapper<Object, Text, Text, Text> {
        private Text station = new Text();
        private Text coValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("StationID_StationName_Month")) {
                return; // Skip the header line
            }
            String[] parts = line.split("\t");
            if (parts.length < 2) {
                return; // Skip this line because it does not have enough fields
            }
            String[] stationMonthParts = parts[0].split("_");
            String stationId = stationMonthParts[0];
            String stationName = stationMonthParts[1];
            String monthYear = stationMonthParts[2];

            String stationKey = stationId + "_" + stationName;
            station.set(stationKey);
            coValue.set(monthYear + "_" + parts[1]);
            context.write(station, coValue);
        }
    }

    // Second Reducer with Header and Annual Average Filtering
    public static class FilterReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(new Text("StationID_StationName"), new Text("Month\tAverage CO Concentration\tAnnual Average CO Concentration"));
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> monthlyData = new ArrayList<>();
            float annualSum = 0;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("_");
                String monthYear = parts[0];
                float coValue = Float.parseFloat(parts[1]);

                monthlyData.add(monthYear + "_" + coValue);
                annualSum += coValue;
                count++;
            }

            if (count == 0) {
                return; // Skip if there were no valid values
            }

            float annualAvg = annualSum / count;

            for (String data : monthlyData) {
                String[] parts = data.split("_");
                String monthYear = parts[0];
                float coValue = Float.parseFloat(parts[1]);

                if (coValue >= annualAvg) {
                    result.set(monthYear + "\t" + coValue + "\t" + annualAvg);
                    context.write(key, result);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "CO concentration");
        job1.setJarByClass(CO_Map_Reduce2.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(FloatSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/user/laur/intermediate_output"));

        boolean firstJobComplete = job1.waitForCompletion(true);
        if (!firstJobComplete) {
            System.exit(1);
        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "CO concentration filter");
        job2.setJarByClass(CO_Map_Reduce2.class);
        job2.setMapperClass(AvgMapper.class);
        job2.setReducerClass(FilterReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("/user/laur/intermediate_output"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
