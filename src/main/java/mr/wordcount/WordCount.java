package mr.wordcount;

import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static utils.MrUtil.SYSTEM_STATUS_FAILED;
import static utils.MrUtil.SYSTEM_STATUS_IMPROPER;
import static utils.MrUtil.SYSTEM_STATUS_NORMAL;

/**
 * @Author JiangZhihao
 * @ClassName WordCount
 * @Description
 * @Date 2019/10/20 0:58
 */
public class WordCount {

  private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

  private final static IntWritable one = new IntWritable(1);

  private static final String jobName = "Word Count";

  private static final int paramCount = 2;

  public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
    logger.debug("run main() ... ");
    if(args.length != paramCount) {
      System.out.println("need params : <inputPath> <outputPath>");
      System.exit(SYSTEM_STATUS_FAILED);
    }

    final String inputPath = args[0];
    final String outputPath = args[1];

    Configuration configuration = new Configuration();
    FileSystem fileSystem=FileSystem.get(new URI(inputPath),configuration);
    if (fileSystem.exists(new Path(outputPath))) {
      fileSystem.delete(new Path(outputPath),true);
    }
    Job job = Job.getInstance(configuration, jobName);

    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    System.exit(job.waitForCompletion(true) ? SYSTEM_STATUS_NORMAL : SYSTEM_STATUS_IMPROPER);
  }

  public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while(tokenizer.hasNext()) {
        context.write(new Text(tokenizer.next()), one);
      }
    }

  }

  public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      for(IntWritable intWritable : values) {
        count += intWritable.get();
      }
      context.write(key, new IntWritable(count));
    }

  }

}
