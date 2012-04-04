package examples;

import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * User: absharma
 * Date: 4/4/12
 */
public class WordCountUnitTests extends TestCase {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Mapper<LongWritable, Text, Text, IntWritable> mapper = new WordCount.Map();
        Reducer<Text, IntWritable, Text, IntWritable> reducer = new WordCount.Reduce();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>(mapper);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>(mapper, reducer);
    }

    @Test
    public void testMap() {
        mapDriver.withInput(new LongWritable(1), new Text("a an the an"))
                .withOutput(new Text("a"), new IntWritable(1))
                .withOutput(new Text("an"), new IntWritable(1))
                .withOutput(new Text("the"), new IntWritable(1))
                .withOutput(new Text("an"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new LongWritable(1), new Text("a an the an"))
                .withOutput(new Text("a"), new IntWritable(1))
                .withOutput(new Text("an"), new IntWritable(2))
                .withOutput(new Text("the"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapReduceMultipleInputs() {
        mapReduceDriver
                .withInput(new LongWritable(1), new Text("a an the an"))
                .withInput(new LongWritable(2), new Text("the an the a"))
                .withOutput(new Text("a"), new IntWritable(2))
                .withOutput(new Text("an"), new IntWritable(3))
                .withOutput(new Text("the"), new IntWritable(3))
                .runTest();
    }
}
