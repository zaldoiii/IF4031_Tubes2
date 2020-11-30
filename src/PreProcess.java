import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PreProcess {

  public static class PreProcessMapper extends Mapper<Object, Text, LongWritable, LongWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String st[] = line.split("\\s");
      Long angka_user = Long.parseLong(st[0]);
      Long angka_follower = Long.parseLong(st[1]);

      LongWritable user = new LongWritable(angka_user);
      LongWritable follower = new LongWritable(angka_follower);

      context.write(user, follower);
      context.write(follower, user);
  }

  public static class PreProcessReducer extends Reducer<LongWritable,LongWritable,LongWritable,Text> {
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      Set<LongWritable> temp = new HashSet<LongWritable>();

      for (LongWritable val : values) {
        LongWritable writable = new LongWritable();
        writable.set(val.get());
        temp.add(writable);
      }
      ArrayList<LongWritable> value_list = new ArrayList<>(temp);

      for (int i = 0; i < value_list.size(); i++) {
        long follower_temp = value_list.get(i).get();
	      String follower_string = String.valueOf(follower_temp);
	      Text follower = new Text(follower_string);
        context.write(key,follower);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "preprocess");
    job.setJarByClass(PreProcess.class);
    job.setMapperClass(PreProcessMapper.class);
    job.setReducerClass(PreProcessReducer.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
