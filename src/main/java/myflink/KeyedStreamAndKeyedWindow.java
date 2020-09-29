package myflink;

import myflink.utils.ListTimedIterator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 用来演示如何使用KeyedStream 和 KeyedWindow 以及 KeyedWindow计算和NonKeyedWindow计算的区别
 * */
public class KeyedStreamAndKeyedWindow {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    List<Integer> ints = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      ints.add(i);
    }

    Iterator<Integer> it = new ListTimedIterator(ints, 500);

    DataStream<Integer> intStream = env.fromCollection(it, Integer.class);

    // 情况一：
    doNonKeyedWindowComputation(intStream);

    // 情况二：
    // doKeyedWindowComputation(intStream);

    // 情况三：
    // keyedWindowThenNonKeyedWindow(intStream);

    env.execute("KeyedStreamAndKeyedWindow");
  }

  private static void doNonKeyedWindowComputation(DataStream<Integer> intStream) {

    intStream
      .countWindowAll(10)
      .max(0)
      // [笔记]
      // Non-Keyed Stream 的Window计算的并行度是1，不允许设置 > 1 的并行度，否则会报错。
      // java.lang.IllegalArgumentException: The parallelism of non parallel operator must be 1.
      // .setParallelism(2)
      .print();
  }

  private static void doKeyedWindowComputation(DataStream<Integer> intStream) {

    intStream.keyBy(new KeySelector<Integer, Integer>() {
      @Override
      public Integer getKey(Integer value) throws Exception {
        return value % 2;
      }
    })
      // [Note]
      // 在KeyedStream中，countWindow中的Size，指的是每个Key对应的size，不是所有的size
      .countWindow(10)
      .max(0)
      .setParallelism(2)
      .print();

    // 但是，在这里，我们可以看到，如果你想要的是全局的最大值(max)，最终输出的结果并不符合你的期望。
  }

  private static void keyedWindowThenNonKeyedWindow(DataStream<Integer> intStream) {

    // 这种方法的好处是，先用KeyedWindow计算提高并行度，再用NonKeyedWindow计算完成全局业务逻辑计算，整体计算速度更快。
    intStream.keyBy(new KeySelector<Integer, Integer>() {
      @Override
      public Integer getKey(Integer value) throws Exception {
        return value % 2;
      }
    })
      .countWindow(10)
      .max(0)
      .setParallelism(6)
      .countWindowAll(10)
      .max(0)
      .print();

  }
}
