import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Query_3_1 {

    public static class JoinMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable customerKey = new IntWritable();
        private final static Text data = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length == 6) { // Customer (ID, Name, Age, Gender, CountryCode, Salary)
                int customerID = Integer.parseInt(fields[0]);
                String customerData = String.format("%s,%s,%s", "CUST", fields[1], fields[5]); // Name, Salary
                customerKey.set(customerID);
                data.set(customerData);
            }
            else if (fields.length == 5) { // Transaction (TransID, CustID, TransTotal, TransNumItems, TransDesc)
                int customerID = Integer.parseInt(fields[1]);
                String transactionData = String.format("%s,%s,%s", "TRANS", fields[2], fields[3]); // TransTotal, TransNumItems
                customerKey.set(customerID);
                data.set(transactionData);
            }
            context.write(customerKey, data);
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "";
            double salary = 0.0;
            int numTransactions = 0;
            double totalSum = 0.0;
            int minItems = Integer.MAX_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");

                if (parts[0].equals("CUST")) {
                    name = parts[1];
                    salary = Double.parseDouble(parts[2]);
                } else if (parts[0].equals("TRANS")) {
                    numTransactions++;
                    totalSum += Double.parseDouble(parts[1]);
                    int numItems = Integer.parseInt(parts[2]);
                    minItems = Math.min(minItems, numItems);
                }
            }

            if (minItems == Integer.MAX_VALUE)
                minItems = 0; // If no transactions, set MinItems to 0

            String result = String.format("%s,%s,%s,%.2f,%s", name, salary, numTransactions, totalSum, minItems);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Transaction Join");

        job.setJarByClass(Query_3_1.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input Path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output Path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
