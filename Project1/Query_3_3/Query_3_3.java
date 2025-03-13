import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class Query_3_3 {

    public static class AgeGroupMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static HashMap<Integer, String> customerAgeGenderMap = new HashMap<>();
        private final static Text ageGroupGenderKey = new Text();
        private final static DoubleWritable outputValue = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException {
            // Retrieve the cached file paths
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                Path cachedFilePath = new Path(cacheFiles[0].getPath());

                // Open the file and load into memory
                BufferedReader reader = new BufferedReader(new FileReader(cachedFilePath.getName()));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    int custID = Integer.parseInt(fields[0]);  // Customer ID
                    int age = Integer.parseInt(fields[2]); // Age
                    String gender = fields[3]; // Gender
                    customerAgeGenderMap.put(custID, age + "," + gender);
                }
                reader.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Should be Transactions dataset only
            int custID = Integer.parseInt(fields[1]);  // CustID
            double transTotal = Double.parseDouble(fields[2]);  // Transaction Total

            String[] ageGender = customerAgeGenderMap.get(custID).split(",");
            int age = Integer.parseInt(ageGender[0]);
            String gender = ageGender[1];
            ageGroupGenderKey.set(getAgeGroup(age) + " " + gender);
            outputValue.set(transTotal);
            context.write(ageGroupGenderKey, outputValue);
        }

        private String getAgeGroup(int age) {
            if (age >= 10 && age < 20) return "[10,20)";
            if (age >= 20 && age < 30) return "[20,30)";
            if (age >= 30 && age < 40) return "[30,40)";
            if (age >= 40 && age < 50) return "[40,50)";
            if (age >= 50 && age < 60) return "[50,60)";
            if (age >= 60 && age <= 70) return "[60,70]";
            return "Unknown";
        }
    }

    public static class AgeGroupReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double minTransTotal = Double.MAX_VALUE;
            double maxTransTotal = Double.MIN_VALUE;
            double sumTransTotal = 0;
            int count = 0;

            for (DoubleWritable val: values) {
                double transTotal = val.get();
                minTransTotal = Math.min(minTransTotal, transTotal);
                maxTransTotal = Math.max(maxTransTotal, transTotal);
                sumTransTotal += transTotal;
                count++;
            }

            // Handle missing transactions (if any from randomized dataset)
            if (minTransTotal == Double.MAX_VALUE) minTransTotal = 0;
            if (maxTransTotal == Double.MIN_VALUE) maxTransTotal = 0;

            double avgTransTotal = count > 0 ? sumTransTotal / count : 0;

            String result = String.format("%.2f,%.2f,%.2f", minTransTotal, maxTransTotal, avgTransTotal);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Country Transaction Report");

        job.addCacheFile(new URI(args[0])); //only customer for caching

        job.setJarByClass(Query_3_3.class);
        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));   // transaction data set only
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output Path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
