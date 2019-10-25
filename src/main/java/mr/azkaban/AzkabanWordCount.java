package mr.azkaban;

import azkaban.jobtype.javautils.AbstractHadoopJob;
import azkaban.utils.Props;
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

/**
 * @Author JiangZhihao
 * @ClassName AzkabanWordCount
 * @Description
 * @Date 2019/10/25 15:26
 */
public class AzkabanWordCount extends AbstractHadoopJob {

  private static final Logger logger = LoggerFactory.getLogger(AzkabanWordCount.class);

  private final String input;

  private final String output;

  private final Boolean overwrite;

  private final static IntWritable one = new IntWritable(1);

  public AzkabanWordCount(String name, Props props) {
    super(name, props);
    this.input = props.get("input.path");
    this.output = props.get("output.path");
    this.overwrite = props.getBoolean("output.overwrite", false);
    logger.info("input:{}, output:{}, overwrite:{}", input, output, overwrite);
  }

  @Override
  public void run() throws Exception {

    Configuration configuration = new Configuration();
    FileSystem fileSystem=FileSystem.get(new URI(input),configuration);
    if (overwrite && fileSystem.exists(new Path(output))) {
      fileSystem.delete(new Path(output),true);
    }

    final String jobName = "azkaban_wordcount_job";
    Job job = Job.getInstance(configuration, jobName);

    job.setJarByClass(AzkabanWordCount.class);
    job.setMapperClass(AzkabanWordCount.WordCountMapper.class);
    job.setReducerClass(AzkabanWordCount.WordCountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    super.run();
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
