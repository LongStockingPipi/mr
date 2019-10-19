package mr.temperature;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author JiangZhihao
 * @ClassName MaxTempreatureMapper
 * @Description
 * @Date 2019/10/19 21:14
 *
 * 泛型：<LongWritable, Text, Text, IntWritable>分别是：
 * 输入map的key和value类型，输出map的key和value的类型
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

  /**
   * @param key 输入map的key
   * @param value 输入map的value
   * @param context 输出map的输入口
   */
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] columns = line.split("\t");
    if(columns.length == 7) {
      String year = columns[2];
      Float maxTemperature = Float.parseFloat(columns[6]);
      context.write(new Text(year), new FloatWritable(maxTemperature));
    }
  }

}
