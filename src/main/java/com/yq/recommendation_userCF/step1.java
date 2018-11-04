package com.yq.recommendation_userCF;

import com.yq.wordcount.FileUtil;
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
import java.util.HashMap;
import java.util.Map;

public class step1 {
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] values = value.toString().split(",");
            String userID = values[0];
            String itemID = values[1];
            String score = values[2];

            outKey.set(userID);
            outValue.set(itemID+"_"+score);
            context.write(outKey,outValue);

        }
    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String userID = key.toString();
            Map<String, Integer> map = new HashMap<String, Integer>();

            for(Text value: values){
                String itemID = value.toString().split("_")[0];
                String score = value.toString().split("_")[1];
                if(map.get(itemID)==null){
                    map.put(itemID, Integer.valueOf(score));
                }else{
                    map.put(itemID, map.get(itemID)+Integer.valueOf(score));
                }

            }
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<String, Integer> entry: map.entrySet()){
                String itemID = entry.getKey();
                String score = String.valueOf(entry.getValue());
                sb.append(itemID+"_"+score+",");

            }
            String line = sb.substring(0,sb.length()-1);
            outKey.set(userID);
            outValue.set(line);
            context.write(outKey,outValue);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step1");
        job.setJarByClass(com.yq.recommendation_userCF.step1.class);
        job.setMapperClass(com.yq.recommendation_userCF.step1.Mapper1.class);
        job.setReducerClass(com.yq.recommendation_userCF.step1.Reduce1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
