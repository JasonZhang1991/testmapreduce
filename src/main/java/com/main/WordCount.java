/**
 * Created by zhangxq1201 on 2018/4/4.
 */
package com.main;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zhangxq1201 on 2018/4/4.
 */
public class WordCount {
    public  static class  TextSplitMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final  static IntWritable one =new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws
                IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
               word.set(itr.nextToken());
               context.write(word, one);
            }
        }
    }
    public  static class IntSumReducer extends  Reducer<Text,IntWritable ,Text,IntWritable>{
        public  void reduce(Text key, Iterable<IntWritable> values, Context context)throws
                IOException, InterruptedException{
            int  sum=0;
            for (IntWritable val:values){
                sum+=val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    public  static void main(String[]  args) throws IOException, ClassNotFoundException, InterruptedException {
        String strInput = "hello, map reduce , nice!";
        StringTokenizer itr = new StringTokenizer(strInput);
        while(itr.hasMoreTokens()){
            System.out.println(itr.nextToken());
        }
        if (args.length!=2){
            System.out.println("请输入输入路径与输出路径");
            System.exit(-1);
        }
        Job  job=Job.getInstance();
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TextSplitMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
