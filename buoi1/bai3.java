package bai3;

import java.io.IOException;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.w3c.dom.Text;
import org.apache.hadoop.io.Text;

public class bai3 {

    // ================= JOB 1 =================
    // Rating Mapper (key = UserID)
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] f = value.toString().split(",");

            if (f.length >= 3) {
                int userId = Integer.parseInt(f[0].trim());
                String movieId = f[1].trim();
                String rating = f[2].trim();

                context.write(new IntWritable(userId), new Text("R|" + movieId + "|" + rating));
            }
        }
    }

    // User Mapper (key = UserID)
    public static class UserMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] f = value.toString().split(",");

            if (f.length >= 2) {
                int userId = Integer.parseInt(f[0].trim());
                String gender = f[1].trim();

                context.write(new IntWritable(userId), new Text("U|" + gender));
            }
        }
    }

    // Reducer Job1: join User + Rating
    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String gender = "";
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("U|")) {
                    gender = v.substring(2);
                } else if (v.startsWith("R|")) {
                    ratings.add(v.substring(2)); // movieId|rating
                }
            }

            if (gender.equals("")) return;

            for (String r : ratings) {
                String[] parts = r.split("\\|");
                int movieId = Integer.parseInt(parts[0]);
                String rating = parts[1];

                context.write(
                        new IntWritable(movieId),
                        new Text(gender + "|" + rating)
                );
            }
        }
    }

    // ================= JOB 2 =================

    // Mapper đọc output job1
    public static class Job1OutputMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\t");

            if (parts.length >= 2) {
                int movieId = Integer.parseInt(parts[0]);
                context.write(new IntWritable(movieId), new Text("G|" + parts[1]));
            }
        }
    }

    // Movie Mapper
    public static class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] f = value.toString().split(",");

            if (f.length >= 2) {
                int movieId = Integer.parseInt(f[0].trim());
                String title = f[1].trim();

                context.write(new IntWritable(movieId), new Text("M|" + title));
            }
        }
    }

    // Reducer Job2: tính avg theo gender
    public static class FinalReducer extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String title = "";
            double maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("M|")) {
                    title = v.substring(2);
                } else if (v.startsWith("G|")) {
                    String[] p = v.substring(2).split("\\|");
                    String gender = p[0];
                    double rating = Double.parseDouble(p[1]);

                    if (gender.equals("M")) {
                        maleSum += rating;
                        maleCount++;
                    } else {
                        femaleSum += rating;
                        femaleCount++;
                    }
                }
            }

            if (title.equals("")) return;

            double maleAvg = maleCount == 0 ? 0 : maleSum / maleCount;
            double femaleAvg = femaleCount == 0 ? 0 : femaleSum / femaleCount;

            context.write(
                    new Text(title),
                    new Text(String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg))
            );
        }
    }

    // ================= MAIN =================
    public static void main(String[] args) throws Exception {

        // ===== JOB 1 =====
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Join User + Rating");

        job1.setJarByClass(bai3.class);
        job1.setReducerClass(JoinReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UserMapper.class);

        Path tempOutput = new Path("temp_output");
        FileOutputFormat.setOutputPath(job1, tempOutput);

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ===== JOB 2 =====
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Final Avg by Gender");

        job2.setJarByClass(bai3.class);
        job2.setReducerClass(FinalReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, tempOutput, TextInputFormat.class, Job1OutputMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}