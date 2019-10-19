package mr.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
 * @ClassName MaxTemprature
 * @Description
 * @Date 2019/10/19 21:49
 */
public class MaxTemperature {

  private static final Logger logger = LoggerFactory.getLogger(MaxTemperature.class);

  private static final String jobName = "Max Temperature";

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

    logger.debug("create job ... ");
    Job job = Job.getInstance(configuration, jobName);

    logger.debug("config job ... ");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(MaxTemperature.class);
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setReducerClass(MaxTemperatureReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    System.exit(job.waitForCompletion(true) ? SYSTEM_STATUS_NORMAL : SYSTEM_STATUS_IMPROPER);

  }

}
