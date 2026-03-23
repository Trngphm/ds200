package bai2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class bai2 {

    // Mapper ratings
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] f = value.toString().split(",");

            if (f.length >= 3) {
                int movieId = Integer.parseInt(f[1].trim());
                String rating = f[2].trim();

                context.write(new IntWritable(movieId), new Text("R|" + rating));
            }
        }
    }

    // Mapper movies
    public static class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] f = value.toString().split(",");

            if (f.length >= 3) {
                int movieId = Integer.parseInt(f[0].trim());
                String genres = f[2].trim();

                context.write(new IntWritable(movieId), new Text("M|" + genres));
            }
        }
    }

    // Reducer
    public static class GenreReducer extends Reducer<IntWritable, Text, Text, Text> {

        Map<String, Double> sumMap = new HashMap<>();
        Map<String, Integer> countMap = new HashMap<>();

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String genres = "";
            List<Double> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("M|")) {
                    genres = v.substring(2);
                } else if (v.startsWith("R|")) {
                    ratings.add(Double.parseDouble(v.substring(2)));
                }
            }

            // Nếu không join được thì bỏ
            if (genres.equals("") || ratings.isEmpty()) return;

            String[] genreList = genres.split("\\|");

            for (String g : genreList) {
                for (double r : ratings) {

                    sumMap.put(g, sumMap.getOrDefault(g, 0.0) + r);
                    countMap.put(g, countMap.getOrDefault(g, 0) + 1);
                }
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (String g : sumMap.keySet()) {

                double avg = sumMap.get(g) / countMap.get(g);

                context.write(
                    new Text(g),
                    new Text("Avg: " + String.format("%.2f", avg) 
                        + ", Count: " + countMap.get(g))
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis");

        job.setJarByClass(bai2.class);

        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ratings1 + ratings2
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        // movies
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
} 