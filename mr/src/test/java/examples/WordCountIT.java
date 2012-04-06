package examples;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * User: absharma
 * Date: 4/5/12
 */
public class WordCountIT {

    private static Path TEST_ROOT_DIR =
            new Path(System.getProperty("test.build.data", "testOut"));
    
    private static Configuration conf = new Configuration();
    private static FileSystem localFs;

    static {
        try {
            localFs = FileSystem.getLocal(conf);
        } catch (IOException io) {
            throw new RuntimeException("problem getting local fs", io);
        }
    }

    @Before
    public void setUp() {
    }

//    @Test
    public void test() throws Exception{
        File f = new File("E:\\code\\mr-ville\\mr\\.\\userlogs\\job_20120406191009873_0001\\attempt_20120406191009873_0001_m_000002_1");
        if(!f.mkdir()) {
            throw new IOException("Mkdirs failed to create "                     + f.toString());
        }
    }
    @Test
    public void testRun() throws Exception {
        MiniMRCluster mr = null;
        try {
            mr = new MiniMRCluster(11112, 11113, 1, "file:///", 3);
            Configuration conf = mr.createJobConf();
            runWordCount(conf);
        } finally {
            if (mr != null) {
                mr.shutdown();
            }
        }

    }

    public static Path writeFile(String name, String data) throws IOException {
        Path file = new Path(TEST_ROOT_DIR + "/" + name);
        localFs.delete(file, false);
        DataOutputStream f = localFs.create(file);
        f.write(data.getBytes());
        f.close();
        return file;
    }

    public static String readFile(String name) throws IOException {
        DataInputStream f = localFs.open(new Path(TEST_ROOT_DIR + "/" + name));
        BufferedReader b = new BufferedReader(new InputStreamReader(f));
        StringBuilder result = new StringBuilder();
        String line = b.readLine();
        while (line != null) {
            result.append(line);
            result.append('\n');
            line = b.readLine();
        }
        b.close();
        return result.toString();
    }

    public static class TrackingTextInputFormat extends TextInputFormat {

        public static class MonoProgressRecordReader extends LineRecordReader {
            private float last = 0.0f;
            private boolean progressCalled = false;

            @Override
            public float getProgress() {
                progressCalled = true;
                final float ret = super.getProgress();
                Assert.assertTrue("getProgress decreased", ret >= last);
                last = ret;
                return ret;
            }

            @Override
            public synchronized void close() throws IOException {
                Assert.assertTrue("getProgress never called", progressCalled);
                super.close();
            }
        }

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context) {
            return new MonoProgressRecordReader();
        }
    }

    private void runWordCount(Configuration conf
    ) throws IOException,
            InterruptedException,
            ClassNotFoundException {
        final String COUNTER_GROUP = "org.apache.hadoop.mapred.Task$Counter";
        localFs.delete(new Path(TEST_ROOT_DIR + "/in"), true);
        localFs.delete(new Path(TEST_ROOT_DIR + "/out"), true);
        writeFile("in/part1", "this is a test\nof word count test\ntest\n");
        writeFile("in/part2", "more test");
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.Map.class);
        job.setCombinerClass(WordCount.Reduce.class);
        job.setReducerClass(WordCount.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TrackingTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(TEST_ROOT_DIR + "/in"));
        FileOutputFormat.setOutputPath(job, new Path(TEST_ROOT_DIR + "/out"));
        Assert.assertNull(job.getJobID());
        Assert.assertTrue(job.waitForCompletion(false));
        Assert.assertNotNull(job.getJobID());
        String out = readFile("out/part-r-00000");
        System.out.println(out);
        Assert.assertEquals("a\t1\ncount\t1\nis\t1\nmore\t1\nof\t1\ntest\t4\nthis\t1\nword\t1\n",
                out);
        Counters ctrs = job.getCounters();
        System.out.println("Counters: " + ctrs);
        long combineIn = ctrs.findCounter(COUNTER_GROUP,
                "COMBINE_INPUT_RECORDS").getValue();
        long combineOut = ctrs.findCounter(COUNTER_GROUP,
                "COMBINE_OUTPUT_RECORDS").getValue();
        long reduceIn = ctrs.findCounter(COUNTER_GROUP,
                "REDUCE_INPUT_RECORDS").getValue();
        long mapOut = ctrs.findCounter(COUNTER_GROUP,
                "MAP_OUTPUT_RECORDS").getValue();
        long reduceOut = ctrs.findCounter(COUNTER_GROUP,
                "REDUCE_OUTPUT_RECORDS").getValue();
        long reduceGrps = ctrs.findCounter(COUNTER_GROUP,
                "REDUCE_INPUT_GROUPS").getValue();
        Assert.assertEquals("map out = combine in", mapOut, combineIn);
        Assert.assertEquals("combine out = reduce in", combineOut, reduceIn);
        Assert.assertTrue("combine in > combine out", combineIn > combineOut);
        Assert.assertEquals("reduce groups = reduce out", reduceGrps, reduceOut);
        String group = "Random Group";
        CounterGroup ctrGrp = ctrs.getGroup(group);
        Assert.assertEquals(0, ctrGrp.size());
    }

}
