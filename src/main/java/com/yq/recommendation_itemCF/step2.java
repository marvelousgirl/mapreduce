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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.abs;

public class step2 {
    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        private List<String> cacheList = new ArrayList<String>();
        private DecimalFormat df = new DecimalFormat("0.00");
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
            String [] userID_score = values[1].split(",");

            double denominator1 = 0.0;
            for(String each : userID_score){
                String score = each.split("_")[1];
                denominator1 += Double.valueOf(score)*Double.valueOf(score);
            }
            denominator1 = Math.sqrt(denominator1);

            for(String line: cacheList){
                String itemID_matrix2 = line.split("\t")[0];
                String [] userID_score_matrix2 = line.split("\t")[1].split(",");

                double denominator2 = 0.0;
                for(String each : userID_score_matrix2){
                    String score = each.split("_")[1];
                    denominator2 += Double.valueOf(score)*Double.valueOf(score);
                }
                denominator2 = Math.sqrt(denominator2);


                int numerator = 0;
                for(String userID_score_value : userID_score){
                    String userID_matrix1 = userID_score_value.split("_")[0];
                    String score_matrix1 = userID_score_value.split("_")[1];
                    for( String userID_score_value_matrix2 : userID_score_matrix2){
                        if(userID_score_value_matrix2.startsWith(userID_matrix1+"_")) {
                            String score_matrix2 = userID_score_value_matrix2.split("_")[1];
                            numerator += Integer.valueOf(score_matrix1)*Integer.valueOf(score_matrix2);
                        }
                    }

                }
                double cos = numerator / (denominator1*denominator2);
                if( abs(cos-0) < 1e-6) continue;
                outKey.set(itemID);
                outValue.set(itemID_matrix2+"_"+df.format(cos));
                context.write(outKey,outValue);
            }

        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text>{
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
        Job job = Job.getInstance(conf, "step2");
        job.setJarByClass(com.yq.recommendation_itemCF.step2.class);
        job.setMapperClass(com.yq.recommendation_itemCF.step2.Mapper2.class);
        job.setReducerClass(com.yq.recommendation_itemCF.step2.Reduce2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
