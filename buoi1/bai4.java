package bai4;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class bai4 {

    // ================= JOB 1 =================

    // Rating Mapper
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

    // User Mapper (lấy AGE)
    public static class UserMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] f = value.toString().split(",");

            if (f.length >= 3) {
                int userId = Integer.parseInt(f[0].trim());
                int age = Integer.parseInt(f[2].trim());

                context.write(new IntWritable(userId), new Text("U|" + age));
            }
        }
    }

    // Reducer Job1: join + convert age → ageGroup
    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Integer age = null;
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("U|")) {
                    age = Integer.parseInt(v.substring(2));
                } else if (v.startsWith("R|")) {
                    ratings.add(v.substring(2)); // movieId|rating
                }
            }

            if (age == null) return;

            String ageGroup = getAgeGroup(age);

            for (String r : ratings) {
                String[] parts = r.split("\\|");
                int movieId = Integer.parseInt(parts[0]);
                String rating = parts[1];

                context.write(
                        new IntWritable(movieId),
                        new Text(ageGroup + "|" + rating)
                );
            }
        }

        private String getAgeGroup(int age) {
            if (age < 18) return "0-18";
            else if (age < 35) return "18-35";
            else if (age < 50) return "35-50";
            else return "50+";
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
                context.write(new IntWritable(movieId), new Text("A|" + parts[1]));
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

    // Reducer Job2: avg theo AGE GROUP
    public static class FinalReducer extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String title = "";

            Map<String, Double> sum = new HashMap<>();
            Map<String, Integer> count = new HashMap<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("M|")) {
                    title = v.substring(2);
                } else if (v.startsWith("A|")) {
                    String[] p = v.substring(2).split("\\|");

                    String ageGroup = p[0];
                    double rating = Double.parseDouble(p[1]);

                    sum.put(ageGroup, sum.getOrDefault(ageGroup, 0.0) + rating);
                    count.put(ageGroup, count.getOrDefault(ageGroup, 0) + 1);
                }
            }

            if (title.equals("")) return;

            String[] groups = {"0-18", "18-35", "35-50", "50+"};
            StringBuilder result = new StringBuilder();

            for (String g : groups) {
                if (count.containsKey(g)) {
                    double avg = sum.get(g) / count.get(g);
                    result.append(g)
                          .append(": ")
                          .append(String.format("%.2f", avg))
                          .append(", ");
                }
            }

            // remove dấu ", " cuối
            if (result.length() > 0) {
                result.setLength(result.length() - 2);
            }

            context.write(new Text(title), new Text(result.toString()));
        }
    }

    // ================= MAIN =================
    public static void main(String[] args) throws Exception {

        // ===== JOB 1 =====
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Join User + Rating");

        job1.setJarByClass(bai4.class);
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
        Job job2 = Job.getInstance(conf2, "Final Avg by Age Group");

        job2.setJarByClass(bai4.class);
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