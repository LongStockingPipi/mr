package data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.Random;

/**
 * @Author JiangZhihao
 * @ClassName Main
 * @Description
 * @Date 2019/10/19 20:36
 */
public class Main {

  /**
   * 准备气温数据，每天的最高气温和最低气温
   * 由于防止计算结果相同，因此将数据尽可能多样，精确到小数点后三位
   * @param args
   */
  public static void main(String[] args) {

    File file = new File("src/main/resources/temperature.txt");

    try(Writer write = new FileWriter(file,true)) {
      BufferedWriter bwriter = new BufferedWriter(write);
      LocalDate now = LocalDate.now();
      LocalDate date = LocalDate.of(1900, 1, 1);
      StringBuilder sb = new StringBuilder();
      while(date.isBefore(now)) {
        sb.append("CN\tLiaoNing\t").append(date.getYear()).append("\t")
            .append(date.getMonthValue()).append("\t").append(date.getDayOfMonth()).append("\t")
            //最低温度
            .append(new DecimalFormat("##.###").format(-35 + ((35) * new Random().nextFloat()))).append("\t")
            //最高温度
            .append(new DecimalFormat("##.###").format(30 + ((20) * new Random().nextFloat())));
        bwriter.write(sb.toString());
        bwriter.newLine();
        bwriter.flush();
        sb.delete(0, sb.length());
        date = date.plusDays(1);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
