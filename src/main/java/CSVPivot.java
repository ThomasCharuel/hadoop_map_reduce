import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.StrictMath.toIntExact;

public class CSVPivot {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, LongWritable, MapWritable>{

        private LongWritable column = new LongWritable();
        private MapWritable cell = new MapWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            // Split the line into cells (cells are separated by ',' character in CSV format)
            String[] cells = value.toString().split(",");

            for(int i=0; i<cells.length; i++){
                column.set(i);

                int position = toIntExact(key.get());

                cell.put(new Text("position"), new IntWritable(position));
                cell.put(new Text("cell"), new Text(cells[i]));
                context.write(column, cell);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<LongWritable,MapWritable,LongWritable,Text> {

        private Text result = new Text();

        public void reduce(LongWritable key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            // The CSV line that will be generated within this function
            StringBuilder line = new StringBuilder();

            // Stores the cells. We will use this list to generate the csv line later
            ArrayList<MapWritable> cells = new ArrayList<>();

            for (MapWritable val : values){

                MapWritable cell = new MapWritable();

                IntWritable position = (IntWritable) val.get(new Text("position"));
                Text cellText = (Text) val.get(new Text("cell"));

                cell.put(new Text("position"), position);
                cell.put(new Text("cell"), cellText);
                cells.add(cell);
            }

            // sort cell list by position
            cells.sort((o1, o2) -> {
                int o1Position = ((IntWritable) o1.get(new Text("position"))).get();
                int o2Position = ((IntWritable) o2.get(new Text("position"))).get();
                return Integer.compare(o1Position, o2Position);
            });

            // write the csv line
            for (MapWritable cell : cells){
                //int position = ((IntWritable) val.get(new Text("position"))).get();
                Text cellText = (Text) cell.get(new Text("cell"));
                line.append(cellText).append(",");
            }

            // Remove last character
            if(line.length() > 0){
                line.setLength(line.length() - 1);
            }

            result.set(line.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CSV pivot");
        job.setJarByClass(CSVPivot.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MapWritable.class);

        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        // configuration contains reference to the named node
        FileSystem fs = FileSystem.get(conf);

        // remove output folder if he exists (otherwise the map/reduce could not be performed)
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
