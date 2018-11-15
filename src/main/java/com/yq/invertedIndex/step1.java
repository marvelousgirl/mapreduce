package com.yq.invertedIndex;

import com.yq.wordcount.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class step1 {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] values = value.toString().split("\\s+");
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            int index = fileName.lastIndexOf(".");
            fileName = fileName.substring(0,index);
            for(String each: values){
                outKey.set(each+"-->"+fileName);
                outValue.set(1);
                context.write(outKey,outValue);
            }

        }
    }

    public static class Combiner1 extends Reducer<Text, LongWritable, Text, LongWritable>{
        public void reduce(Text key,Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
            long cnt = 0;
            for(LongWritable value: values){
                cnt += value.get();
            }
            context.write(key, new LongWritable(cnt));
        }

    }

    public static class Partitioner1 extends HashPartitioner<Text,LongWritable> {
        public int getPartition(Text key, LongWritable value, int numReduceTasks){
               String [] tokens = key.toString().split("-->");
               return super.getPartition(new Text(tokens[0]),value,numReduceTasks);
        }
    }

    public static class Reducer1 extends Reducer<Text, LongWritable, Text, Text>{
        private Map<String, Long> map = new LinkedHashMap<String, Long>();
        private String preTerm = null;
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
            String [] tokens = key.toString().split("-->");
            if(preTerm==null)
                preTerm = tokens[0];
            if(preTerm.equals(tokens[0])){
                String fileName = tokens[1];
                long cnt = 0;
                for(LongWritable value: values ){
                    cnt += value.get();
                }
                map.put(fileName,cnt);
            }else{
                String str = "";
                double aveFreq = 0.0;
                for(Map.Entry<String, Long> entry: map.entrySet()){
                    str+=entry.getKey()+"："+entry.getValue()+",";
                    aveFreq += (double)entry.getValue();

                }
                aveFreq /= (double)map.size();
                str = str.substring(0,str.length()-1);

                context.write(new Text(preTerm),new Text(str));
                context.write(new Text(preTerm), new Text(""+aveFreq));
                preTerm = tokens[0];
                map.clear();
                String fileName = tokens[1];
                long cnt = 0;
                for(LongWritable value: values ){
                    cnt += value.get();
                }
                map.put(fileName,cnt);

            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException{
            String str = "";
            double aveFreq = 0.0;
            for(Map.Entry<String, Long> entry: map.entrySet()){
                str+=entry.getKey()+"："+entry.getValue()+",";
                aveFreq += (double)entry.getValue();

            }
            aveFreq /= (double)map.size();
            str = str.substring(0,str.length()-1);

            context.write(new Text(preTerm),new Text(str));
            context.write(new Text(preTerm), new Text(""+aveFreq));

        }

    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InverseIndex");
        // job.setNumReduceTasks(2);
        job.setJarByClass(com.yq.invertedIndex.step1.class);
        job.setMapperClass(com.yq.invertedIndex.step1.Mapper1.class);
        job.setCombinerClass(com.yq.invertedIndex.step1.Combiner1.class);
        job.setPartitionerClass(com.yq.invertedIndex.step1.Partitioner1.class);
        job.setReducerClass(com.yq.invertedIndex.step1.Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
