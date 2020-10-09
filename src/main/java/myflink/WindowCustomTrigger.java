package myflink;

import lombok.Data;
import myflink.utils.ListTimedIterator;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 演示如何通过自定义Trigger来实现在窗口结束之前，输出Window聚合计算结果
 * Window计算结果输出的触发条件有三个：
 *   1. 当window中element个数到一定的量(maxCount)，相当于{@link org.apache.flink.streaming.api.windowing.triggers.CountTrigger}
 *   2. 当window中EventTime超过一段时间(interval)，相当于{@link org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger}
 *   3. 当Window.end_time < currentWatermark，相当于{@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger}
 * */
public class WindowCustomTrigger {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // TODO: 生成Watermark的Interval对Trigger是否触发有影响。
    env.getConfig().setAutoWatermarkInterval(1000);


    // 模拟数据源，连续输出event timestamp = [1, 100] 的 Event，每隔一秒输出一次。
    List<Event> events = new ArrayList<>();
    for (int i = 0; i <= 100; i++) {
      events.add(new Event(
        i * Time.seconds(1).toMilliseconds(),
        ThreadLocalRandom.current().nextInt()));
    }

    Iterator<Event> it = new ListTimedIterator(events, Time.seconds(1).toMilliseconds());

    // Assign timestamp and Watermarks
    DataStream<Event> eventStream = env.fromCollection(it, Event.class);

    // 抽取Timestamp.
    DataStream<Event> withTimestampAndWatermark = eventStream
      .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {

        final Duration delay = Duration.ofMillis(0);

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
          return new BoundedOutOfOrdernessWatermarks<>(delay);
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {

          return new TimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
              return element.getEventTime();
            }
          };
        }
      });

    withTimestampAndWatermark
      // 以EventTime = 1 seconds 为单位的window计算
      .timeWindowAll(Time.seconds(10))
      // 每EventTime的Watermark经过 10 mills 或者 收到5个event触发一次计算。
      .trigger(CountAndContinuousEventTimeTrigger.of(Time.seconds(7), 3))
      // 计算当前window中有多少个element
      .aggregate(new CountElement())
      // 由于最终只生成1个结果，所以sink不需要并行。
      .print().setParallelism(1);

    env.execute("WindowCustomTrigger");

    /**
     * 程序启动后，每过 3，6，9，10 秒，都会输出Window计算结果 count element = 3，6，9，10
     *
     * 这里为什么是9而不是7呢？请参考{@link BoundedOutOfOrdernessWatermarks}
     * 与 {@link org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger}
     * 的实现.
     * */
  }

  @Data
  public static class Event implements Serializable {
    private long eventTime = 0;
    private int value = 0;

    // Rules for POJO types that Flink Support:
    //   1. The class has a public no-argument constructor
    //   2. The class must be public.
    //   3. All fields are either public or must be accessible through getter and setter functions.
    //     For a field called foo the getter and setter methods must be named getFoo() and setFoo().
    //   4. The type of a field must be supported by a registered serializer.
    // see more: https://ci.apache.org/projects/flink/flink-docs-stable/dev/types_serialization.html
    public Event() {}

    public Event(long eventTime, int value) {
      this.eventTime = eventTime;
      this.value = value;
    }
  }

  /**
   * Count the number of {@link Event}
   * */
  private static class CountElement implements AggregateFunction<Event, Long, Long> {

    @Override
    public Long createAccumulator() {
      return Long.valueOf(0);
    }

    @Override
    public Long add(Event value, Long accumulator) {
      return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
      return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
      return a + b;
    }
  }
}
