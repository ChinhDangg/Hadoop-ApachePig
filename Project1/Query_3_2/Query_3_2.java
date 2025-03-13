import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class Query_3_2 {

    public static class CountryMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static HashMap<Integer, Integer> customerCountryMap = new HashMap<>();
        private final static IntWritable countryCode = new IntWritable();
        private final static Text outputValue = new Text();

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
                    int country = Integer.parseInt(fields[4]); // CountryCode
                    customerCountryMap.put(custID, country);
                }
                reader.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length == 6) {
                int country = Integer.parseInt(fields[4]);  // CountryCode
                countryCode.set(country);
                outputValue.set("C");
            }
            else if (fields.length == 5) {  // Transactions dataset
                int custID = Integer.parseInt(fields[1]);  // CustID
                double transTotal = Double.parseDouble(fields[2]);  // Transaction Total

                // Lookup CountryCode from HashMap
                int country = customerCountryMap.get(custID);
                countryCode.set(country);
                outputValue.set("T," + transTotal);
            }
            context.write(countryCode, outputValue);
        }
    }

    public static class CountryReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int customerCount = 0;
            double minTransTotal = Double.MAX_VALUE;
            double maxTransTotal = Double.MIN_VALUE;

            for (Text val: values) {
                String[] parts = val.toString().split(",");

                if (parts[0].equals("C")) {
                    customerCount++;
                }
                else if (parts[0].equals("T")) {  // Transaction Record
                    double transTotal = Double.parseDouble(parts[1]);
                    minTransTotal = Math.min(minTransTotal, transTotal);
                    maxTransTotal = Math.max(maxTransTotal, transTotal);
                }
            }

            // Handle missing transactions (if any from randomized dataset)
            if (minTransTotal == Double.MAX_VALUE) minTransTotal = 0;
            if (maxTransTotal == Double.MIN_VALUE) maxTransTotal = 0;

            String result = String.format("%s,%s,%s", customerCount, minTransTotal, maxTransTotal);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Country Transaction Report");

        job.addCacheFile(new URI(args[0])); //only customer for caching

        job.setJarByClass(Query_3_2.class);
        job.setMapperClass(CountryMapper.class);
        job.setReducerClass(CountryReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));   // both customer and transaction
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output Path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
