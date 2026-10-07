package edu.course.bigdata.mapreduce;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 对child-parent关系执行自连接，输出grandchild-grandparent关系。 */
public final class Grandparent {
    private Grandparent() {
    }

    public static class RelationMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] fields = line.split("\\s+");
            if (fields.length < 2) {
                context.getCounter("Experiment1", "INVALID_RELATION").increment(1L);
                return;
            }
            String child = fields[0];
            String parent = fields[1];
            if ("child".equalsIgnoreCase(child) && "parent".equalsIgnoreCase(parent)) {
                return;
            }

            // 以parent为中间人，记录其直接孩子。
            outputKey.set(parent);
            outputValue.set("C\t" + child);
            context.write(outputKey, outputValue);

            // 以child为中间人，记录其直接父母。
            outputKey.set(child);
            outputValue.set("P\t" + parent);
            context.write(outputKey, outputValue);
        }
    }

    public static class RelationReducer extends Reducer<Text, Text, Text, Text> {
        private final Text grandchildOutput = new Text();
        private final Text grandparentOutput = new Text();

        @Override
        protected void reduce(Text middlePerson, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> children = new LinkedHashSet<String>();
            Set<String> parents = new LinkedHashSet<String>();

            for (Text value : values) {
                String text = value.toString();
                String[] fields = text.split("\\t", 2);
                if (fields.length != 2) {
                    continue;
                }
                if ("C".equals(fields[0])) {
                    children.add(fields[1]);
                } else if ("P".equals(fields[0])) {
                    parents.add(fields[1]);
                }
            }

            for (String child : children) {
                for (String parent : parents) {
                    grandchildOutput.set(child);
                    grandparentOutput.set(parent);
                    context.write(grandchildOutput, grandparentOutput);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("用法：Grandparent <输入路径> <输出路径>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(), "experiment1-grandparent");
        job.setJarByClass(Grandparent.class);
        job.setMapperClass(RelationMapper.class);
        job.setReducerClass(RelationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

