import org.apache.hadoop.conf.Configuration;
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
            }
            catch (URISyntaxException e) {
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

    public static class SeoGrouper extends WritableComparator {
        protected SeoGrouper() {
            super(SEOcomposite.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((SEOcomposite) a).getUrl().compareTo(((SEOcomposite) b).getUrl());
        }
    }

    public static class SeoComparator extends WritableComparator {

        SeoComparator() {
            super(SEOcomposite.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((SEOcomposite) a).compareTo((SEOcomposite) b);
        }
    }

    public static class SEOreduceer extends Reducer<SEOcomposite, NullWritable, Text, IntWritable> {

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
            System.out.println("Two parameters are required for SeoMain");
            return -1;
        }

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("SEO_OPTIMIZATION");

        job.setJarByClass(SEOmain.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SEOmapper.class);
        job.setMapOutputKeyClass(SEOcomposite.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setPartitionerClass(SEOpartitioner.class);
        job.setSortComparatorClass(SeoComparator.class);
        job.setGroupingComparatorClass(SeoGrouper.class);

        job.setReducerClass(SEOreduceer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}
