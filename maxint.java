package maxint;
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
 *
 * Last modified by: Andrey Danilkovich
 * Class: CS422 - Data mining
 * Illinois Institute of Technology
 * Max integer
 */

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class maxint {

  public static class TokenizerMapper 
       extends Mapper<LongWritable,Text,Text,IntWritable>{
    
    // create new text with empty string
    private Text file = new Text("0");
    // make it writable with function
    private IntWritable number = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // read string line
    	String line = value.toString();
        // set default max to 0
        int max = 0;
        // split the string by space separator
      	String[] tokens = line.split(" ");
        // enter loop
      	for(String token : tokens){
            // parse the token
      		  int i = Integer.parseInt(token);
      		  if(i > max){
      			  max = i;
      		  }
      	  }
      	number.set(max);
      	String output = Integer.toString(max);
      	//Text final_output = Text.set(output);
      	Text final_output = new Text(output);
        // key-value pair: pass function with the key (file name) and max integer (result)
        context.write(file, number); 
      }
  }
  
  /* test case - obscelete */
  public static class MaxIntReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int max = 0;
      for (IntWritable val : values) {
    	  int j = val.get();
    	  if(j > max){
    		  max = j;		  
    	  }
      }
      result.set(max);
      // key-value pair: pass function with the key and max integer
      // key-value pair: file name, max integer
      context.write(key, result);
    }
  }

  /* reduce function to compare the integers and collect the largest number from each file */
  public void reduce(K key, Iterator<V> values, Context context)
    throws IOException {
      //set v max to next value
      V max = values.next();
      // evaluate
      while( values.hasNext() ) {
          // set to current value
          V current = values.next();
          // check if max, then override
          if( current.compareTo(max) > 0 )
              max = current;
      }
      // key-value pair: pass function with the key and max integer
      context.collect(key, max);
  }

  /* main function */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: max integer <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "max integer");
    job.setJarByClass(maxint.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(MaxIntReducer.class);
    job.setReducerClass(MaxIntReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
