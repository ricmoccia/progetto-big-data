import java.io.IOException;
import java.util.*;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job1 {

    public static class CarMapper extends Mapper<LongWritable, Text, Text, Text> {
        private CSVParser parser;

        @Override
        protected void setup(Context context) {
            parser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                System.err.println("DEBUG line: " + line);

                String[] fields = parser.parseLine(line);
                if (fields.length < 9) {
                    System.err.println("SKIP: not enough fields (" + fields.length + ")");
                    return;
                }

                String make = fields[5];
                String model = fields[6];
                String price = fields[7];
                String year = fields[8];

                if (make.equalsIgnoreCase("make_name")) {
                    System.err.println("SKIP: header line");
                    return;
                }

                System.err.println("EMIT: " + make + "\t" + model + " -> " + price + "\t" + year);
                context.write(new Text(make + "\t" + model), new Text(price + "\t" + year));

            } catch (Exception e) {
                System.err.println("ERROR: " + e.getMessage());
            }
        }
    }

    public static class CarReducer extends Reducer<Text, Text, Text, Text> {
       public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    System.err.println("REDUCER RECEIVE: " + key.toString());

    int count = 0;
    double sum = 0.0;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    String year = "";

    for (Text val : values) {
        String[] parts = val.toString().split("\t");
        if (parts.length != 2) continue;

        try {
            double price = Double.parseDouble(parts[0]);
            year = parts[1];

            count++;
            sum += price;
            if (price < min) min = price;
            if (price > max) max = price;
        } catch (NumberFormatException e) {
            System.err.println("Invalid number format in value: " + val.toString());
        }
    }

    if (count > 0) {
        double avg = sum / count;
        String result = count + "\t" + min + "\t" + max + "\t" + String.format("%.2f", avg) + "\t[" + year + "]";
        System.err.println("REDUCER EMIT: " + key.toString() + " -> " + result);
        context.write(key, new Text(result));
    } else {
        System.err.println("REDUCER SKIPPED: " + key.toString() + " (no valid values)");
    }
}

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Job1 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job1");
        job.setJarByClass(Job1.class);
        job.setMapperClass(CarMapper.class);
        job.setReducerClass(CarReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
