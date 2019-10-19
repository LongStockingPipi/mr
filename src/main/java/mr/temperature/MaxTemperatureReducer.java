package mr.temperature;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author JiangZhihao
 * @ClassName MaxTempreature
 * @Description
 * @Date 2019/10/19 21:41
 */
public class MaxTemperatureReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

  @Override
  protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
    Float maxTemp = Float.MIN_VALUE;
    for(FloatWritable input : values) {
      maxTemp = Math.max(maxTemp, input.get());
    }
    context.write(key, new FloatWritable(maxTemp));
  }


}
