import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Q3_2 {

    private static final Logger LOG = Logger.getLogger(Q3_2.class);

    public static class Point implements Writable {
        int x, y;

        public Point() {}
        public Point(int x, int y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(x);
            out.writeInt(y);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            x = in.readInt();
            y = in.readInt();
        }

        public static Point parse(String line) {
            String[] parts = line.split("[ ,-]+"); // Handles comma, space, and dash as delimiters
            return new Point(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        }

        public double distance(Point other) {
            return Math.sqrt(Math.pow(x - other.x, 2) + Math.pow(y - other.y, 2));
        }

        @Override
        public String toString() {
            return x + "," + y;
        }
    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        List<Point> centers = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path centersPath = new Path(conf.get("centersPath"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centersPath)));
            String line;
            while ((line = reader.readLine()) != null) {
                centers.add(Point.parse(line));
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Point point = Point.parse(value.toString());
            double minDistance = Double.MAX_VALUE;
            int closestCenterIdx = -1;
            for (int i = 0; i < centers.size(); i++) {
                double distance = point.distance(centers.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCenterIdx = i;
                }
            }
            context.write(new IntWritable(closestCenterIdx), point);
        }
    }

    public static class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            long sumX = 0;
            long sumY = 0;
            int count = 0;
            for (Point p : values) {
                sumX += p.x;
                sumY += p.y;
                count++;
            }
            context.write(key, new Point((int)(sumX / count), (int)(sumY / count)));
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Point, NullWritable, Text> {

        private static final int distanceDifference = 1;
        List<Point> newCenters = new ArrayList<>();

        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            long sumX = 0, sumY = 0;
            int count = 0;
            for (Point p : values) {
                sumX += p.x;
                sumY += p.y;
                count++;
            }
            Point newCenter = new Point((int)(sumX / count), (int)(sumY / count));
            newCenters.add(newCenter);

            Configuration conf = context.getConfiguration();
            Path centersPath = new Path(conf.get("centersPath"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(centersPath)));
            List<Point> oldCenters = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                oldCenters.add(Point.parse(line));
            }
            reader.close();

            Point oldCenter = oldCenters.get(key.get());
            String status = oldCenter.distance(newCenter) >= distanceDifference ? "CHANGED" : "UNCHANGED";
            if (status.equals("CHANGED")) {
                context.getCounter("KMeans", "CHANGED").increment(1);
            }
            context.write(NullWritable.get(), new Text(newCenter.toString() + " - " + status));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path centersPath = new Path(args[1]); // k-points path

        int maxIterations = 6;
        int iteration = 0;
        long changed;
        do {
            conf.set("centersPath", centersPath.toString());

            Job job = Job.getInstance(conf, "KMeans Iteration " + iteration);
            job.setJarByClass(Q3_2.class);

            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path outputPath = new Path(args[2] + "/iteration_" + iteration);
            fs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.waitForCompletion(true);
            changed = job.getCounters().findCounter("KMeans", "CHANGED").getValue();
            centersPath = new Path(outputPath + "/part-r-00000"); // Update the centers path for the next iteration
            iteration++;
        } while (changed > 0 && iteration < maxIterations);
    }
}
