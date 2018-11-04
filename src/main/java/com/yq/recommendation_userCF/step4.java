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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.abs;

public class step4 {
    public static class Mapper4 extends Mapper<LongWritable, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        private List<String> cacheList = new ArrayList<String>();
        private DecimalFormat df = new DecimalFormat("0.00");
        protected void setup(Context context ) throws IOException, InterruptedException{
            super.setup(context);
            FileReader fr = new FileReader("output/userCF/output3/part-r-00000");
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
            String userID = values[0];
            String [] user_sim = values[1].split(",");

            for(String line: cacheList){
                String itemID_matrix2 = line.split("\t")[0];
                String [] userID_score_matrix2 = line.split("\t")[1].split(",");

                double numerator = 0.0;
                for(String userID_score_value : user_sim){
                    String userID_matrix1 = userID_score_value.split("_")[0];
                    String score_matrix1 = userID_score_value.split("_")[1];
                    for( String userID_score_value_matrix2 : userID_score_matrix2){
                        if(userID_score_value_matrix2.startsWith(userID_matrix1+"_")) {
                            String score_matrix2 = userID_score_value_matrix2.split("_")[1];
                            numerator += Double.valueOf(score_matrix1)*Double.valueOf(score_matrix2);
                        }
                    }

                }
                if( abs(numerator-0) < 1e-6) continue;
                outKey.set(userID);
                outValue.set(itemID_matrix2+"_"+df.format(numerator));
                context.write(outKey,outValue);
            }

        }
    }

    public static class Reduce4 extends Reducer<Text, Text, Text, Text>{
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
        Job job = Job.getInstance(conf, "step4");
        job.setJarByClass(com.yq.recommendation_userCF.step4.class);
        job.setMapperClass(com.yq.recommendation_userCF.step4.Mapper4.class);
        job.setReducerClass(com.yq.recommendation_userCF.step4.Reduce4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
