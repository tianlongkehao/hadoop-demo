package com.hadoop.wang;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Average {
	
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {  
        // 实现map函数  
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
            // 将输入的纯文本文件的数据转化成String  
            String line = value.toString();  
            // 将输入的数据首先按行进行分割  
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");  
            // 分别对每一行进行处理  
            while (tokenizerArticle.hasMoreElements()) {  
                // 每行按空格划分  
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());  
                String strName = tokenizerLine.nextToken();// 学生姓名部分  
                String strScore = tokenizerLine.nextToken();// 成绩部分  
                Text name = new Text(strName);  
                int scoreInt = Integer.parseInt(strScore);  
                // 输出姓名和成绩  
                context.write(name, new IntWritable(scoreInt));  
            }  
        }  
    }
    
    
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {  
        // 实现reduce函数  
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {  
            int sum = 0;  
            int count = 0;  
            Iterator<IntWritable> iterator = values.iterator();  
            while (iterator.hasNext()) {  
                sum += iterator.next().get();// 计算总分  
                count++;// 统计总的科目数  
            }  
            int average = (int) sum / count;// 计算平均成绩  
            context.write(key, new IntWritable(average));  
        }  
    }
    
    
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        //设置hadoop的机器、端口  
        conf.set("mapred.job.tracker", "192.168.223.128:9000");  
        //设置输入输出文件目录  
        String[] ioArgs = new String[] { "hdfs://wangmaster:9000/average_in", "hdfs://wangmaster:9000/average_out" };  
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();  
        if (otherArgs.length != 2) {  
            System.err.println("Usage: Score Average <in> <out>");  
            System.exit(2);  
        }  
        //设置一个job  
        Job job = Job.getInstance(conf, "Score Average");  
          
        //去除重复的输出文件夹  
//        FileSystem fs = FileSystem.get(conf);  
//        Path out = new Path(otherArgs[1]);  
//        if (fs.exists(out)){  
//            fs.delete(out, true);  
//        }  
          
        job.setJarByClass(Average.class);  
          
        // 设置Map、Combine和Reduce处理类  
        job.setMapperClass(Map.class);  
        job.setCombinerClass(Reduce.class);  
        job.setReducerClass(Reduce.class);  
          
        // 设置输出类型  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
          
        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现  
        job.setInputFormatClass(TextInputFormat.class);  
          
        // 提供一个RecordWriter的实现，负责数据输出  
        job.setOutputFormatClass(TextOutputFormat.class);  
          
        // 设置输入和输出目录  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
  
    } 

}
