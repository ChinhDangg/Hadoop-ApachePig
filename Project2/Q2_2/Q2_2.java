import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Q2_2 {

    private static final Logger LOG = Logger.getLogger(Q2_2.class);

    public static class PointWritable implements Writable {
        public int x, y;

        public PointWritable() {}

        public PointWritable(int x, int y) {
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

        @Override
        public String toString() {
            return x + "," + y;
        }

        public double distance(PointWritable other) {
            return Math.sqrt(Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2));
        }
    }

    // Mapper class
    public static class OutlierMapper extends Mapper<LongWritable, Text, Text, PointWritable> {
        private int gridSize;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            gridSize = Integer.parseInt(conf.get("radius")); // Set grid size based on radius
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            int x = Integer.parseInt(parts[0]);
            int y = Integer.parseInt(parts[1]);

            PointWritable point = new PointWritable(x, y);

            int cellX = x / gridSize;
            int cellY = y / gridSize;

            // Emit the point to its own cell and neighboring cells
            for (int i = -1; i <= 1; i++) {
                for (int j = -1; j <= 1; j++) {
                    String cellKey = (cellX + i) + "," + (cellY + j);
                    context.write(new Text(cellKey), point);
                }
            }
        }
    }

    // Reducer class
    public static class OutlierReducer extends Reducer<Text, PointWritable, Text, Text> {
        private int r;
        private int k;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            r = Integer.parseInt(conf.get("radius"));
            k = Integer.parseInt(conf.get("threshold"));
        }

        @Override
        protected void reduce(Text key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
            List<PointWritable> points = new ArrayList<>();

            // Collect points in the same cell and neighbors
            for (PointWritable p : values) {
                points.add(new PointWritable(p.x, p.y));
            }

            // Check neighbors within radius for each point
            for (PointWritable p1 : points) {
                int neighborCount = 0;

                for (PointWritable p2 : points) {
                    if (!p1.equals(p2) && p1.distance(p2) <= r) {
                        neighborCount++;
                    }
                }

                if (neighborCount < k) {
                    context.write(null, new Text(p1.toString()));
                }
//                else {
//                    context.write(new Text(p1.toString()), new Text("INLIER"));
//                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Please provide arguments for OutlierDetection: <input path> <output path> <int-radius r> <int-threshold k>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("radius", args[2]);
        conf.set("threshold", args[3]);

        Job job = Job.getInstance(conf, "Outlier Detection");
        job.setJarByClass(Q2_2.class);

        job.setMapperClass(OutlierMapper.class);
        job.setReducerClass(OutlierReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PointWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(8); // Use 8 reducers

        // use 4 mappers ? since points dataset is only 100mb, so using more splits
        FileInputFormat.setMaxInputSplitSize(job, 28 * 1024 * 1024);  // 28 MB
        FileInputFormat.setMinInputSplitSize(job, 28 * 1024 * 1024);  // 28 MB

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
