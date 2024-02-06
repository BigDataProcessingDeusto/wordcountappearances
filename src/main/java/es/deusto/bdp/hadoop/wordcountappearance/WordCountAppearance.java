package es.deusto.bdp.hadoop.wordcountappearance;

import java.io.IOException;
import java.lang.Comparable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;

public class WordCountAppearance {
    public static class WordCountMapper
       extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context
                   ) throws IOException, InterruptedException {
            
	        String[] line = value.toString().split("\\s");
            IntWritable one = new IntWritable(1);

            for (int i=0; i < line.length; i++) {
                context.write(new Text(line[i]), one);
            }

        }
    }

    public static class WordCountReducer
       extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

            int result = 0;
            for (IntWritable value : values) {
                result += value.get();
            }   
            context.write(key, new IntWritable(result));
        }
    }

    public static class CountAppearanceMapper
        extends Mapper<Object, Text, Text, IntWritable> {
        
            public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

                String[] tuple = value.toString().split("\t");
                IntWritable one = new IntWritable(1);
                context.write(new Text(tuple[1]), one);

            }
    }

    public static class CountAppearanceReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

            public void reduce(Text key, Iterable<IntWritable> values, 
                            Context context
                            ) throws IOException, InterruptedException {

                int result = 0;
                for (IntWritable value : values) {
                    result += value.get();
                }
                context.write(key, new IntWritable(result));
            }
    }

    public static class Tuple implements Comparable<Tuple> {
        private String key;
        private int value;

        public Tuple() { }
        public Tuple(String key, int value) {
            this.key = key;
            this.value = value;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public String getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }

        @Override
        public int compareTo(Tuple otherTuple) {
            return Integer.compare(this.value, otherTuple.getValue());
        }
    }

    public static class AppearanceSortingMapper
        extends Mapper<Object, Text, Text, Text> {

            public void map(Object Key, Text value, Context context
                        ) throws IOException, InterruptedException {

                context.write(new Text("result"), value);

            }

    }

    public static class AppearanceSortingReducer
        extends Reducer<Text, Text, Text, IntWritable> {
                public void reduce(Text key, Iterable<Text> values, Context context
                            ) throws IOException, InterruptedException {

                    List<Tuple> results = new ArrayList<Tuple>();

                    for (Text value : values) {
                        String[] strTuple = value.toString().split("\t");

                        Tuple tuple = new Tuple(strTuple[0], Integer.parseInt(strTuple[1]));
                        results.add(tuple);
                    }

                    Collections.sort(results);
                    Collections.reverse(results);
                    for (Tuple result : results) {
                        context.write(new Text(result.getKey()), new IntWritable(result.getValue()));
                    }
                }
        }

     public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "WordCount");
        job1.setJarByClass(WordCountAppearance.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setCombinerClass(WordCountReducer.class);
        job1.setReducerClass(WordCountReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "AppearanceCount");
        job2.setJarByClass(WordCountAppearance.class);
        job2.setMapperClass(CountAppearanceMapper.class);
        job2.setCombinerClass(CountAppearanceReducer.class);
        job2.setReducerClass(CountAppearanceReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf, "AppearanceSorting");
        job3.setJarByClass(WordCountAppearance.class);
        job3.setMapperClass(AppearanceSortingMapper.class);
        //job3.setCombinerClass(AppearanceSortingReducer.class);
        job3.setReducerClass(AppearanceSortingReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
