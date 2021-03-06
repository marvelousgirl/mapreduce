package com.yq.recommendation_itemCF;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class step5 {
    public static class Mapper5 extends Mapper<LongWritable, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        private List<String> cacheList = new ArrayList<String>();
        protected void setup(Context context ) throws IOException, InterruptedException{
            super.setup(context);
            FileReader fr = new FileReader("output/itemCF/output1/part-r-00000");
            BufferedReader br = new BufferedReader(fr);

            String line = null;
            while((line = br.readLine())!=null){
                cacheList.add(line);
            }

            br.close();
            fr.close();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] values = value.toString().split("\t");
            String itemID = values[0];
            String [] user_score_array = values[1].split(",");

            for(String line: cacheList){
                String itemID_matrix2 = line.split("\t")[0];
                String [] user_score_array_matrix2 = line.split("\t")[1].split(",");

                if(itemID_matrix2.equals(itemID)){
                    for(String user_score_value : user_score_array){
                        boolean flag = false;
                        String userID_matrix1 = user_score_value.split("_")[0];
                        String score_matrix1 = user_score_value.split("_")[1];
                        for( String userID_score_value_matrix2 : user_score_array_matrix2){
                            if(userID_score_value_matrix2.startsWith(userID_matrix1+"_")) {
                                flag = true;
                            }
                        }
                        if(flag == false){
                            outKey.set(userID_matrix1);
                            outValue.set(itemID+"_"+score_matrix1);
                            context.write(outKey,outValue);
                        }


                    }
                }

            }

        }
    }

    public static class Reduce5 extends Reducer<Text, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            StringBuilder sb = new StringBuilder();
            for(Text value: values)
                sb.append(value+",");
            String line = sb.substring(0,sb.length()-1);
            outKey.set(key);
            outValue.set(line);
            context.write(outKey,outValue);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        FileUtil.deleteDir(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step5");
        job.setJarByClass(com.yq.recommendation_itemCF.step5.class);
        job.setMapperClass(com.yq.recommendation_itemCF.step5.Mapper5.class);
        job.setReducerClass(com.yq.recommendation_itemCF.step5.Reduce5.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
