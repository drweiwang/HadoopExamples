package com.mathworks.weiw.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Created by Wei Wang on 8/14/14.
 * Simple Hadoop MapReduce example to compute the mean arrival delay by day of week using the airline on-time performance dataset
 */

public class GroupedMeanApp{

  public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       if (otherArgs.length != 2) {
           System.err.println("Usage: GroupedMeanApp <in> <out>");
           System.exit(2);
       }
       Job job = new Job(conf, "GroupedMeanApp");
       job.setJarByClass(GroupedMeanApp.class);
       job.setMapperClass(GrpMeanMapper.class);
       job.setCombinerClass(GrpMeanReducer.class);
       job.setReducerClass(GrpMeanReducer.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(ArrayPrimitiveWritable.class);
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
       job.setOutputFormatClass(SequenceFileOutputFormat.class);


       boolean completionStatus = job.waitForCompletion(true);
      if (completionStatus) {
          FileSystem fs = FileSystem.get(conf);
          SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(otherArgs[1],"part-r-00000"), conf);
          Writable key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
          ArrayPrimitiveWritable value = (ArrayPrimitiveWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

          System.out.println("\n\nMean Arrival Delay By Day of Week:");
          System.out.println("DayOfWeek\tMeanDelay");
          while (reader.next(key, value)) {
              double[] sumCount = (double[]) value.get();
              System.out.println(key + "\t" + sumCount[0] / sumCount[1]);
          }
      }
      else{
          System.exit(0);
      }
  }


    public static class GrpMeanMapper extends Mapper<LongWritable, Text, Text, ArrayPrimitiveWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String[] fields = value.toString().split(",");
            Double arrDelay;
            if (fields[14].matches("[-+]?[0-9]*.?[0-9]+")) {
                arrDelay = Double.parseDouble(fields[14]);
            }
            else{
                return;
            }

            String dayOfWeek = fields[3];
            ArrayPrimitiveWritable intermVal = GroupedMeanApp.createDoubleArrayWritable(arrDelay, 1.0);
            context.write(new Text(dayOfWeek), intermVal);
        }
    }

    public static class GrpMeanReducer extends Reducer<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable>{
        public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context)
                throws IOException, InterruptedException{

            Double sum = 0.0;
            Double count = 0.0;

            for (ArrayPrimitiveWritable val : values){
                sum += ((double[]) val.get())[0];
                count += ((double[]) val.get())[1];
            }
            context.write(key, GroupedMeanApp.createDoubleArrayWritable(sum, count));
        }

    }

    private static ArrayPrimitiveWritable createDoubleArrayWritable(Double arrDelay, Double count){
        double [] vals = {arrDelay, count};
        ArrayPrimitiveWritable intermVal = new ArrayPrimitiveWritable();
        intermVal.set(vals);
        return intermVal;
    }
}
