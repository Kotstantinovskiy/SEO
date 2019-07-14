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

    public static class SEOmapper extends Mapper<LongWritable, Text, SEOcomposite, NullWritable> {

        @Override
        public void map(LongWritable key, Text host_query, Context context) throws IOException, InterruptedException {
            System.out.println("Start mapper");
            String host_query_str = host_query.toString();
            String[] splits = host_query_str.split("\t");

            if(splits.length != 2){
                return;
            }

            String host;
            String query = splits[0];

            try {
                host = new URI(splits[1]).getHost();
            } catch (URISyntaxException e) {
                return;
            }

            if(host == null) {
                return;
            }
            context.write(new SEOcomposite(host, query), NullWritable.get());
        }
    }

    public static class SEOpartitioner extends Partitioner<SEOcomposite, NullWritable> {
        @Override
        public int getPartition(SEOcomposite key, NullWritable val, int numPartitions) {
            return Math.abs(key.getUrl().hashCode()) % numPartitions;
        }
    }

    public static class SEOComparatorGroup extends WritableComparator {
        protected SEOComparatorGroup() {
            super(SEOcomposite.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((SEOcomposite) a).getUrl().compareTo(((SEOcomposite) b).getUrl());
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

    public static class SEOreducer extends Reducer<SEOcomposite, NullWritable, Text, IntWritable> {

        @Override
        public void reduce(SEOcomposite pair, Iterable<NullWritable> nulls, Context context) throws IOException, InterruptedException {
            System.out.println("Start reducer");
            String bestQuery = "", currentQuery = "";
            int currentCounter = 0, bestCounter = 0;
            String url = pair.getUrl().toString();

            for(NullWritable a : nulls) {
                String query = pair.getQuery().toString();

                if(bestQuery.equals("")) {
                    bestQuery = query;
                }

                if(!query.equals(currentQuery)) {
                    currentQuery = query;
                    currentCounter = 1;
                }

                currentCounter++;

                if(currentCounter > bestCounter) {
                    bestCounter = currentCounter;
                    bestQuery = currentQuery;
                }
            }

            if (bestCounter >= context.getConfiguration().getLong(Config.MIN_CLICKS, 1)) {
                context.write(new Text(url + "\t" + bestQuery), new IntWritable(bestCounter));
            }
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
        System.out.println("11111111111111111111111111");
        Job job = Job.getInstance(getConf());
        job.setJobName("SEO_OPTIMIZATION");

        job.setJarByClass(SEOmain.class);
        System.out.println("22222222222222222222222222");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("33333333333333333333333333");
        job.setMapperClass(SEOmapper.class);
        job.setMapOutputKeyClass(SEOcomposite.class);
        job.setMapOutputValueClass(NullWritable.class);
        System.out.println("44444444444444444444444444");
        job.setPartitionerClass(SEOpartitioner.class);
        job.setSortComparatorClass(SEOComparatorSort.class);
        job.setGroupingComparatorClass(SEOComparatorGroup.class);
        System.out.println("55555555555555555555555555");
        job.setReducerClass(SEOreducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("66666666666666666666666666");
        job.setNumReduceTasks(Config.REDUCE_COUNT);
        System.out.println("77777777777777777777777777");
        boolean success = job.waitForCompletion(true);
        System.out.println(success);
        return success ? 0 : 1;
    }
}
