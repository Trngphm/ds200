package bai1;

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

public class bai1 {

    // Mapper đọc ratings
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private IntWritable movieId = new IntWritable();
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length >= 3) {

                int movie = Integer.parseInt(fields[1].trim());
                String rating = fields[2].trim();

                movieId.set(movie);
                outValue.set("R:" + rating);

                context.write(movieId, outValue);
            }
        }
    }

    // Mapper đọc movies
    public static class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        IntWritable movieId = new IntWritable();
        Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length >= 2) {

                int movie = Integer.parseInt(fields[0].trim());
                String title = fields[1].trim();

                movieId.set(movie);
                outValue.set("M:" + title);

                context.write(movieId, outValue);
            }
        }
    }

    // Reducer
    public static class MovieReducer extends Reducer<IntWritable, Text, Text, Text> {

        String maxMovie = "";
        double maxRating = 0.0;
        boolean found = false;

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String movieTitle = "";
            double sum = 0;
            int count = 0;

            for (Text val : values) {

                String v = val.toString();

                if (v.startsWith("M:")) {
                    movieTitle = v.substring(2);
                }
                else if (v.startsWith("R:")) {

                    double rating = Double.parseDouble(v.substring(2));
                    sum += rating;
                    count++;
                }
            }

            if (count > 0) {

                double avg = sum / count;

                context.write(
                        new Text(movieTitle),
                        new Text("AverageRating: " + String.format("%.2f", avg)
                                + " (TotalRatings: " + count + ")")
                );

                if (count >= 5 && avg > maxRating) {
                    maxRating = avg;
                    maxMovie = movieTitle;
                    found = true;
                }
            }
        }

        // phim có ít nhất 5 đánh giá và có điểm trung bình cao nhất
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            if (found) {
                context.write(
                        new Text(""),
                        new Text(maxMovie
                                + " là phim có điểm trung bình cao nhất với số điểm đánh giá trung bình là "
                                + String.format("%.2f", maxRating)
                                + " và có ít nhất 5 đánh giá.")
                );
            } else {
                context.write(
                        new Text(""),
                        new Text("Không có phim nào có ít nhất 5 đánh giá.")
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(bai1.class);

        job.setReducerClass(MovieReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job,
                new Path(args[0]),
                TextInputFormat.class,
                RatingMapper.class);

        MultipleInputs.addInputPath(job,
                new Path(args[1]),
                TextInputFormat.class,
                RatingMapper.class);

        MultipleInputs.addInputPath(job,
                new Path(args[2]),
                TextInputFormat.class,
                MovieMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}