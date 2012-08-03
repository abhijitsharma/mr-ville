package examples;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

    private static Path TEST_DIR = new Path("target" + File.separatorChar + "testOut");

    private static Configuration config = new Configuration();
    private static FileSystem lfs;

    static {
        try {
            lfs = FileSystem.getLocal(config);
        } catch (IOException io) {
            throw new RuntimeException("Error : Local fs", io);
        }
    }

    @Before
    public void setUp() {
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

    public static Path writeFile(FileSystem _lfs, String path, String data) throws IOException {
        Path file = new Path(path);
        _lfs.delete(file, false);
        DataOutputStream f = _lfs.create(file);
        f.write(data.getBytes());
        f.close();
        return file;
    }

    public static String readFile(FileSystem _lfs, String path) throws IOException {
        DataInputStream f = _lfs.open(new Path(path));
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

    private void runWordCount(Configuration conf) throws Exception {
        lfs.delete(new Path(TEST_DIR + File.separator + "in"), true);
        lfs.delete(new Path(TEST_DIR + File.separator + "out"), true);
        writeFile(lfs, TEST_DIR + File.separator + "in/part1", "this is a test\nof word count test\ntest\n");
        writeFile(lfs, TEST_DIR + File.separator + "in/part2", "more test");
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.Map.class);
        job.setCombinerClass(WordCount.Reduce.class);
        job.setReducerClass(WordCount.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(TEST_DIR + "/in"));
        FileOutputFormat.setOutputPath(job, new Path(TEST_DIR + "/out"));
        Assert.assertNull(job.getJobID());
        Assert.assertTrue(job.waitForCompletion(false));
        Assert.assertNotNull(job.getJobID());
        String out = readFile(lfs, TEST_DIR + File.separator + "out/part-r-00000");
        System.out.println(out);
        Assert.assertEquals("a\t1\ncount\t1\nis\t1\nmore\t1\nof\t1\ntest\t4\nthis\t1\nword\t1\n",
                out);
        Counters ctrs = job.getCounters();
        System.out.println("Counters: " + ctrs);
    }

}
