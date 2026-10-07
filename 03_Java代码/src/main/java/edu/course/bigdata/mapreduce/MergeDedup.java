package edu.course.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 合并多个输入文件，删除重复行，并按文本键排序输出。 */
public final class MergeDedup {
    private MergeDedup() {
    }

    public static class DedupMapper
            extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (!line.isEmpty()) {
                outputKey.set(line);
                context.write(outputKey, NullWritable.get());
            }
        }
    }

    public static class DedupReducer
            extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法：MergeDedup <输入路径1> [输入路径2 ...] <输出路径>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(), "experiment1-merge-dedup");
        job.setJarByClass(MergeDedup.class);
        job.setMapperClass(DedupMapper.class);
        job.setReducerClass(DedupReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

