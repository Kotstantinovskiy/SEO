import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SEOmain extends Configured implements Tool {

    public static class SEOmapper_1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text host_query, Context context) throws IOException, InterruptedException {
            String host_query_str = host_query.toString();
            String[] splits = host_query_str.split("\t");

            if(splits.length != 2){
                return;
            }

            String query = splits[0];
            String host;
            try {
                host = new URI(splits[1]).getHost();
            } catch (URISyntaxException e) {
                return;
            }

            if(host == null) {
                return;
            }

            context.write(new Text(host + "\t" + query), new IntWritable(1));
        }
    }

    public static class SEOreducer_1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text host_query, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {

            int n = 0;
            for(IntWritable a : ones) {
                n++;
            }

            if (n >= context.getConfiguration().getLong(Config.MIN_CLICKS, 1)) {
                context.write(new Text(host_query), new IntWritable(n));
            }
        }
    }

    public static class SEOmapper_2 extends Mapper<LongWritable, Text, SEOcomposite, Text> {

        @Override
        public void map(LongWritable key, Text host_query_num, Context context) throws IOException, InterruptedException {
            System.out.println("Start mapper");
            String host_query_num_str = host_query_num.toString();
            String[] splits = host_query_num_str.split("\t");

            if(splits.length != 3){
                return;
            }

            String host = splits[0];
            String query = splits[1];
            int num = Integer.valueOf(splits[2]);

            context.write(new SEOcomposite(host, num), new Text(query));
        }
    }

    public static class SEOpartitioner extends Partitioner<SEOcomposite, Text> {
        @Override
        public int getPartition(SEOcomposite key, Text val, int numPartitions) {
            return Math.abs(key.getHost().hashCode()) % numPartitions;
        }
    }

    public static class SEOComparatorGroup extends WritableComparator {
        protected SEOComparatorGroup() {
            super(SEOcomposite.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((SEOcomposite) a).getHost().compareTo(((SEOcomposite) b).getHost());
        }
    }

    public static class SEOComparatorSort extends WritableComparator {

        SEOComparatorSort() {
            super(SEOcomposite.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((SEOcomposite) a).compareTo((SEOcomposite) b);
        }
    }

    public static class SEOreducer_2 extends Reducer<SEOcomposite, Text, Text, IntWritable> {

        @Override
        public void reduce(SEOcomposite key, Iterable<Text> queries, Context context) throws IOException, InterruptedException {
            System.out.println("Start reducer");
            context.write(new Text(key.getHost().toString() + "\t" + queries.iterator().next().toString()), key.getNum());
        }
    }


    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new SEOmain(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two parameters are required!");
            return -1;
        }

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = Job.getInstance(getConf());
        job.setJobName("SEO_OPTIMIZATION_STEP_1");

        job.setJarByClass(SEOmain.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tmp"));

        job.setMapperClass(SEOmapper_1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SEOreducer_1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);

        if(!success)
            return 0;
        //---------------------------------------------------------------
        job = Job.getInstance(getConf());
        job.setJobName("SEO_OPTIMIZATION_STEP_2");

        job.setJarByClass(SEOmain.class);
        FileInputFormat.setInputPaths(job, new Path(args[1] + "/tmp"));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/out"));

        job.setMapperClass(SEOmapper_2.class);
        job.setMapOutputKeyClass(SEOcomposite.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(SEOpartitioner.class);
        job.setSortComparatorClass(SEOComparatorSort.class);
        job.setGroupingComparatorClass(SEOComparatorGroup.class);

        job.setReducerClass(SEOreducer_2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        success = job.waitForCompletion(true);
        fs.delete(new Path(args[1] + "/tmp"), true);
        return success ? 0 : 1;
    }
}
