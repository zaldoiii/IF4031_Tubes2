package TriangleCount;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class TriangleCounter extends Configured implements Tool
{
	// first Mapper
    public static class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
    {
        LongWritable u = new LongWritable();
        LongWritable v = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            long node1,node2;
            if (tokenizer.hasMoreTokens())
            {
                node1 = Long.parseLong(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid data in line" + line);
                node2 = Long.parseLong(tokenizer.nextToken());

                if (node1 < node2)
                {
                    u.set(node1);
                    v.set(node2);
                    context.write(u,v);
                }
            }
        }
    }

    // first Reducer
    public static class TriadsReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable>
    {
        Text rKey = new Text();
        final static LongWritable zero = new LongWritable((byte)0);
        final static LongWritable one = new LongWritable((byte)1);
        long []vArray = new long[4096];
        int size = 0;

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            Iterator<LongWritable> vs = values.iterator();
            for (size = 0; vs.hasNext(); )
            {
                if (vArray.length==size)//add size if array is full
                {
                    vArray = Arrays.copyOf(vArray, vArray.length*2);
                }

                long node = vs.next().get();
                vArray[size++] = node;

                // generates all possible sets of edges.
                rKey.set(key.toString() + "," + Long.toString(node));
                context.write(rKey, zero);
            }

            Arrays.sort(vArray, 0, size);

            // generates all possible triples for a vertex.
            for (int i=0; i<size; ++i)
            {
                for (int j=i+1; j<size; ++j)
                {
                    rKey.set(Long.toString(vArray[i]) + "," + Long.toString(vArray[j]));
                    context.write(rKey, one);
                }
            }
        }
    }

    // second Mapper
    public static class ParseTextLongPairsMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text mKey = new Text();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if (tokenizer.hasMoreTokens())
            {
                mKey.set(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid data2 line " + line);
                mValue.set(Long.parseLong(tokenizer.nextToken()));
                context.write(mKey, mValue);
            }
        }
    }

    // second Reducer
    public static class CountTrianglesReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
        long count = 0;
        final static LongWritable zero = new LongWritable(0);

        public void cleanup(Context context)
            throws IOException, InterruptedException
        {
            LongWritable v = new LongWritable(count);
            if (count > 0) context.write(zero, v);
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            boolean isClosed = false;
            long c = 0, n = 0;
            Iterator<LongWritable> vs = values.iterator();
            while (vs.hasNext())
            {
                c += vs.next().get();
                ++n;
            }
            if (c!=n) count += c;
        }
    }

    // reducer for aggregation
    public static class AggregateCountsReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            long sum = 0;
            Iterator<LongWritable> vs = values.iterator();
            while (vs.hasNext())
            {
                sum += vs.next().get();
            }
            context.write(new LongWritable(sum), null);
        }
    }

    
    public int run(String[] args) throws Exception
    {
    	long startTime = System.nanoTime();
        Job job1 = new Job(getConf());
        job1.setJobName("triads");

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setJarByClass(TriangleCounter.class);
        job1.setMapperClass(ParseLongLongPairsMapper.class);
        job1.setReducerClass(TriadsReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp1"));


        Job job2 = new Job(getConf());
        job2.setJobName("triangles");

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setJarByClass(TriangleCounter.class);
        job2.setMapperClass(ParseTextLongPairsMapper.class);
        job2.setReducerClass(CountTrianglesReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path("temp1"));
        FileOutputFormat.setOutputPath(job2, new Path("temp2"));


        Job job3 = new Job(getConf());
        job3.setJobName("count");

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setJarByClass(TriangleCounter.class);
        job3.setMapperClass(ParseTextLongPairsMapper.class);
        job3.setReducerClass(AggregateCountsReducer.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job3, new Path("temp2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));


        int ret = job1.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job2.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job3.waitForCompletion(true) ? 0 : 1;
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("Estimated Execution Time = " + estimatedTime + " nanoseconds");
        System.out.println("Estimated Execution Time = " + estimatedTime / 1000000000 + " seconds");
        return ret;
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new TriangleCounter(), args);
        System.exit(res);
    }
}