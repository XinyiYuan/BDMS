/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

// Modified by Shimin Chen to demonstrate functionality for Homework 2
// April-May 2015

import java.io.*;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import javax.xml.soap.*;

public class Hw2Part1 {
    
    public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, DoubleWritable>{
        
        private Text src_dst = new Text();
        private final static DoubleWritable cal_time = new DoubleWritable(1);
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            System.out.println("text: " + text);
            String[] lines = text.split("\n");
            
            for(int i=0; i<lines.length; i++) {
                System.out.println("lines[i]: " + lines[i]);
                String[] line = lines[i].split(" ");
                if (line.length == 3){
                    String src = line[0];
                    String dst = line[1];
                    src_dst.set(src + " " + dst);
                    
                    double time = Double.parseDouble(line[2]);
                    cal_time.set(time);
                    
                    System.out.println("src_dst: " + src_dst);
                    System.out.println("cal_time: " + cal_time);
                    
                    context.write(src_dst, cal_time);
                }
            }
        }
    }
    
    public static class AvgTimeReducer
    extends Reducer<Text, DoubleWritable, Text, Text> {
        private final Text result = new Text();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            double sumTime = 0.0;
            double avgTime = 0.0;
            for (DoubleWritable val : values) {
                sum++;
                sumTime += val.get();
            }
            avgTime = sumTime / sum;
            /*
            String[] info = key.toString().split(" ");
            if (info.length == 3){
                result.set(String.format("%s %s %d %.3f", info[0], info[1], sum, avgTime));
                context.write(result, NullWritable.get());
            }
            */
            result.set(sum + " " + String.format("%.3f",avgTime));
            System.out.println("key: " + key);
            System.out.println("result: " + result);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Hw2Part1 <in> [<in>...] <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "Hw2Part1");
        
        job.setJarByClass(Hw2Part1.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(AvgTimeReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // add the input paths as given by command line
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        
        // add the output path as given by the command line
        FileOutputFormat.setOutputPath(job,
            new Path(otherArgs[otherArgs.length - 1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
