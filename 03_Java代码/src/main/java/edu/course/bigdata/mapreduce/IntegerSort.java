package edu.course.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 读取多个整数文件，进行全局升序排序，并输出“位次 整数”。 */
public final class IntegerSort {
    private IntegerSort() {
    }

    public static class SortMapper
            extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
        private final IntWritable number = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            try {
                number.set(Integer.parseInt(line));
                context.write(number, NullWritable.get());
            } catch (NumberFormatException exception) {
                context.getCounter("Experiment1", "INVALID_INTEGER").increment(1L);
                System.err.println("跳过非整数内容：" + line);
            }
        }
    }

    public static class RankReducer
            extends Reducer<IntWritable, NullWritable, LongWritable, IntWritable> {
        private long rank = 1L;
        private final LongWritable outputRank = new LongWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            for (NullWritable ignored : values) {
                outputRank.set(rank++);
                context.write(outputRank, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法：IntegerSort <输入路径1> [输入路径2 ...] <输出路径>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(), "experiment1-integer-sort");
        job.setJarByClass(IntegerSort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(RankReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 只有一个Reducer时，输出文件中的整数才是全局有序的。
        job.setNumReduceTasks(1);

        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

