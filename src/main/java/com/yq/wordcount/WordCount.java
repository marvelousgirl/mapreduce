package com.yq.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        public static final LongWritable one = new LongWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                this.word.set(itr.nextToken());
                context.write(this.word, one);
            }
        }
    }

    public static class SumReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
        private LongWritable result = new LongWritable();
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            LongWritable val;
            for(Iterator i = values.iterator(); i.hasNext(); sum += val.get()){
                val = (LongWritable)i.next();
            }
            this.result.set(sum);
            context.write(key,this.result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setReducerClass(WordCount.SumReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
