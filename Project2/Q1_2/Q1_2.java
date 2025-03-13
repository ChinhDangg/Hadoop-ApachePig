import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Q1_2 {

    private static final Logger LOG = Logger.getLogger(SpatialJoinReducer.class);
    private static final int cellSize = 50;


    public static class PointMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static int[] window;

        @Override
        protected void setup(Context context) {
            String windowParam = context.getConfiguration().get("window");
            if (windowParam != null) {
                String[] parts = windowParam.split(",");
                window = new int[]{Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2]), Integer.parseInt(parts[3])};
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            int x = Integer.parseInt(parts[0]);
            int y = Integer.parseInt(parts[1]);

            // Skip points outside the window
            if (window != null) {
                if (x < window[0] || x > window[2] || y < window[1] || y > window[3]) {
                    return;
                }
            }

            int cellId = computeCell(x, y);
            context.write(new IntWritable(cellId), new Text("P;" + value));
        }

        private int computeCell(int x, int y) {
            //using int is the big idea here, anything within the cellSize will be in that cell size
            int gridX = x / cellSize; // Horizontal cell index
            int gridY = y / cellSize; // Vertical cell index
            return gridX * 10000 + gridY; // Unique cell ID
        }
    }

    public static class RectangleMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static int[] window;

        @Override
        protected void setup(Context context) {
            String windowParam = context.getConfiguration().get("window");
            if (windowParam != null) {
                String[] parts = windowParam.split(",");
                window = new int[]{Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
                        Integer.parseInt(parts[2]), Integer.parseInt(parts[3])};
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            String rectId = parts[0];
            int x = Integer.parseInt(parts[1]); // Top-left x
            int y = Integer.parseInt(parts[2]); // Top-left y
            int w = Integer.parseInt(parts[3]); // Width
            int h = Integer.parseInt(parts[4]); // Height

            int x1 = x;           // Top-left x
            int y1 = y;           // Top-left y
            int x2 = x + w;       // Bottom-right x
            int y2 = y + h;       // Bottom-right y

            // Skip rectangles entirely outside the window
            if (window != null) {
                if (x2 < window[0] || x1 > window[2] || y2 < window[1] || y1 > window[3]) {
                    return;
                }
            }

            // Emit for each cell this rectangle overlaps
            for (int i = x1; i <= x2; i += cellSize) {
                for (int j = y1; j <= y2; j += cellSize) {
                    int cellId = computeCell(i, j);
                    context.write(new IntWritable(cellId), new Text("R;" + value));
                }
            }
        }

        private int computeCell(int x, int y) {
            //using int is the big idea here, anything within the cellSize will be in that cell size
            int gridX = x / cellSize; // Horizontal cell index
            int gridY = y / cellSize; // Vertical cell index
            return gridX * 10000 + gridY; // Unique cell ID
        }
    }

    // Reducer class
    public static class SpatialJoinReducer extends Reducer<IntWritable, Text, Text, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> rectangles = new ArrayList<>();
            List<String> points = new ArrayList<>();

            // Separate points and rectangles
            for (Text val : values) {
                String[] parts = val.toString().split(";");
                if (parts[0].equals("R")) {
                    rectangles.add(parts[1]);
                } else if (parts[0].equals("P")){
                    points.add(parts[1]);
                }
            }

            // Perform join
            for (String rectangle : rectangles) {
                String[] rectParts = rectangle.split(",");
                String rectId = rectParts[0];
                int x1 = Integer.parseInt(rectParts[1]); // Top-left x
                int y1 = Integer.parseInt(rectParts[2]); // Top-left y
                int x2 = x1 + Integer.parseInt(rectParts[3]); // Bottom-right x (x + w)
                int y2 = y1 + Integer.parseInt(rectParts[4]); // Bottom-right y (y + h)

                for (String point : points) {
                    String[] pointParts = point.split(",");
                    int px = Integer.parseInt(pointParts[0]);
                    int py = Integer.parseInt(pointParts[1]);

                    // Check if point lies within the rectangle
                    if (px >= x1 && px <= x2 && py >= y1 && py <= y2) {
                        context.write(new Text(rectId), new Text("(" + px + "," + py + ")"));
                    }
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Optional window parameter
        if (args.length == 6) {
            conf.set("window", args[2] + "," + args[3] + "," + args[4] + "," + args[5]);
        }

        Job job = Job.getInstance(conf, "Spatial Join");
        job.setJarByClass(Q1_2.class);

        // Set up multiple inputs
        MultipleInputs.addInputPath(job, new Path(args[0] + "/points.csv"), TextInputFormat.class, PointMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0] + "/rectangles.csv"), TextInputFormat.class, RectangleMapper.class);

        job.setReducerClass(SpatialJoinReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(8);  // 8 reducers

        // use 4 mappers ? since points and rectangles dataset is only 100mb, so using more splits
        FileInputFormat.setMaxInputSplitSize(job, 28 * 1024 * 1024);  // 28 MB
        FileInputFormat.setMinInputSplitSize(job, 28 * 1024 * 1024);  // 28 MB


        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
