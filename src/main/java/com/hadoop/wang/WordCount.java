package com.hadoop.wang;

import java.io.IOException;  
import java.util.Iterator;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapred.FileInputFormat;  
import org.apache.hadoop.mapred.FileOutputFormat;  
import org.apache.hadoop.mapred.JobClient;  
import org.apache.hadoop.mapred.JobConf;  
import org.apache.hadoop.mapred.MapReduceBase;  
import org.apache.hadoop.mapred.Mapper;  
import org.apache.hadoop.mapred.OutputCollector;  
import org.apache.hadoop.mapred.Reducer;  
import org.apache.hadoop.mapred.Reporter;  
import org.apache.hadoop.mapred.TextInputFormat;  
import org.apache.hadoop.mapred.TextOutputFormat;  
   
public class WordCount {  
   
    public static class WordCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {  
        private final static IntWritable one =new IntWritable(1);  
        private Text word =new Text();  
   
        public void map(Object key,Text value,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {  
            StringTokenizer itr = new StringTokenizer(value.toString());  
            while(itr.hasMoreTokens()) {  
                word.set(itr.nextToken());  
                output.collect(word,one);//字符解析成key-value,然后再发给reducer  
            }  
   
        }  
    }  
   
    public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {  
        private IntWritable result =new IntWritable();  
   
        public void reduce(Text key, Iterator<IntWritable>values, OutputCollector<Text, IntWritable> output, Reporter reporter)throws IOException {  
            int sum = 0;  
            while (values.hasNext()){//key相同的map会被发送到同一个reducer,所以通过循环来累加  
                sum +=values.next().get();  
            }  
            result.set(sum);  
            output.collect(key, result);//结果写到hdfs  
        }  
        
    }  
   
    public static void main(String[] args)throws Exception {  
        //System.setProperty("hadoop.home.dir", "D:\\project\\hadoop-2.7.2");  // 如果本地环境变量没有设置hadoop路径可以这么做
        String input = "hdfs://wangmaster:9000/input/LICENSE.txt";//要确保linux上这个文件存在  
        String output = "hdfs://wangmaster:9000/output2/";  
   
        JobConf conf = new JobConf(WordCount.class);  
        conf.setJobName("WordCount");  
        //方法一设置连接参数  
        conf.addResource("classpath:/hadoop/core-site.xml");  
        conf.addResource("classpath:/hadoop/hdfs-site.xml");  
        conf.addResource("classpath:/hadoop/mapred-site.xml");   
       //方法二设置连接参数  
       //conf.set("mapred.job.tracker", "10.75.201.125:9000");     
   
        conf.setOutputKeyClass(Text.class);  //设置输出key格式
        conf.setOutputValueClass(IntWritable.class);  //设置输出value格式 
   
       conf.setMapperClass(WordCountMapper.class);  //设置Map算子
       conf.setCombinerClass(WordCountReducer.class);  //设置Combine算子 
       conf.setReducerClass(WordCountReducer.class);  //设置reduce算子
   
        conf.setInputFormat(TextInputFormat.class); //设置输入格式 
       conf.setOutputFormat(TextOutputFormat.class); //设置输出格式 
   
        FileInputFormat.setInputPaths(conf,new Path(input)); //设置输入路径
        FileOutputFormat.setOutputPath(conf,new Path(output)); //设置输出路径  
   
        JobClient.runJob(conf);  
        System.exit(0);  
    }  
   
} 
