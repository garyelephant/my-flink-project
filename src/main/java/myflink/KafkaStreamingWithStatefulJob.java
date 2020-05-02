package myflink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

/**
 * 这是一个关于实时从kafka消费数据，并且做一些有状态计算的程序，
 * 场景是：实时流式日志的监控、报警
 * 要求是：
 *    * 消费Kafka数据
 *    * 实时聚合监控指标（窗口内的ErrorLog个数）, 超过阈值就发报警
 *    * 原始日志输出到ES
 *    * 聚合监控指标输出到ES
 *
 * Log Event Format:
 *    [time],[type],[content]
 *      time format: yyyy-MM-dd hh:mm:ss,
 *
 * 通过此程序，你可以学习到：
 * （1） 数据延迟(late data), 乱序(out-of-order)，flink是怎么处理的。
 *
 * */
public class KafkaStreamingWithStatefulJob {

  private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public static void main(String[] args) throws Exception {


    // for exmpale: file:///Users/gary/workspace/flink/log_events
    final String outputPath = args[0];
    final Time windowTime = Time.seconds(5);
    final String kafkaTopic = "access_log";


    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // enable checkpoint:
    //  see: https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kafka.html#kafka-consumers-and-fault-tolerance
    //  see: https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/elasticsearch.html#elasticsearch-sinks-and-fault-tolerance
    env.enableCheckpointing(5000); // checkpoint every 5000 msecs
    env.setParallelism(1); // TODO: 需要某个Operator中，所有Task都对齐了Watermark，才能输出，所以暂时把Parallelism调整为1

    // Note that in order to run this example in event time, the program needs to either
    // use sources that directly define event time for the data and emit watermarks themselves,
    // or the program must inject a Timestamp Assigner & Watermark Generator after the sources.
    // Those functions describe how to access the event timestamps, and what degree of
    // out-of-orderness the event stream exhibits.


    // A stream processor that supports event time needs a way to measure the progress of event time.
    // For example, a window operator that builds hourly windows needs to be notified
    // when event time has passed beyond the end of an hour,
    // so that the operator can close the window in progress.

    // The mechanism in Flink to measure progress in event time is watermarks.
    // Watermarks flow as part of the data stream and carry a timestamp t.
    // A Watermark(t) declares that event time has reached time t in that stream,
    // meaning that there should be no more elements from the stream with a timestamp t’ <= t
    // (i.e. events with timestamps older or equal to the watermark).
    // from: https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/event_time.html#event-time-and-watermarks

    // Time的设置，会直接影响到Window Operation 的行为，如后面看到的DataStream.timeWindow()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    final OutputTag<LogEvent> outputTag = new OutputTag<LogEvent>("aggs"){};

    // [only for debug] DataStream<String> socketStream = env.socketTextStream("localhost", 9999)

    DataStream<String> kafkaStream = env
      .addSource(
        new FlinkKafkaConsumer(
          kafkaTopic,
          new SimpleStringSchema(),
          createKafkaConsumerConfig()));

    // [only for debug] SingleOutputStreamOperator<LogEvent> dataStream = socketStream
    SingleOutputStreamOperator<LogEvent> dataStream = kafkaStream
      .filter(e -> (StringUtils.isNotBlank(e) && e.split(",").length == 3))
      .map(new Splitter())
      .process(new StreamSplitter(outputTag));


    // 思考: 不用side output也可以做到 多路输出（如一个输出原始数据，一个聚合），那么他们的区别是什么？
    // [only for debug] dataStream.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);
    final String logDetailIndexName = "access_log";
    dataStream.addSink(new ElasticsearchSink(logDetailIndexName));

     DataStream<Tuple3<String, Long, Long>> aggsDataStream = dataStream
       .getSideOutput(outputTag)
       .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
      .filter(e -> StringUtils.equalsIgnoreCase(e.eventType, "error"))
      // .keyBy(e -> e.eventUUID) // TODO: keyBy的含义，以及是否可以这样，为什么keyBy需要在filter后面。
      .timeWindowAll(windowTime)
       .apply(new ErrorLogAggregations());

    // So basically there are two possible directions to follow based on the type of calculations
    // you would like to do. Either use: fold/reduce/aggregate or more generic one, you already mentioned -apply.
    // All of them apply to windows for a key.

    // [only for debug] aggsDataStream.print();
    final String logAggsIndexName = "access_log_aggs";
    aggsDataStream.addSink(new ElasticsearchSink(logAggsIndexName));

    env.execute("Window WordCount");

    // TODO: Window聚合定期输出最新结果，甚至允许Queryable
    // TODO: Retract with update / delete，要求数据是一致的。
  }

  public static Properties createKafkaConsumerConfig() {

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181");
    properties.setProperty("group.id", "test");
    return properties;
  }

  /**
   * References:
   *  https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/elasticsearch.html#elasticsearch-sink
   * */
  public static ElasticsearchSink createElasticsearchSink() {
    // TODO:

  }

  public static class StreamSplitter extends ProcessFunction<LogEvent, LogEvent> {

    private OutputTag<LogEvent> tag;

    public StreamSplitter(OutputTag<LogEvent> tag) {
      this.tag = tag;
    }

    @Override
    public void processElement(LogEvent value, Context ctx, Collector<LogEvent> out) throws Exception {
      // emit data to regular output
      out.collect(value);

      // emit data to side output
      ctx.output(tag, value);
    }
  }

  public static class ErrorLogAggregations implements AllWindowFunction<LogEvent, Tuple3<String, Long, Long>, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<LogEvent> values, Collector<Tuple3<String, Long, Long>> out) throws Exception {

      java.util.Date dt =new java.util.Date(window.getStart());


      long cnt = 0;
      Iterator<LogEvent> iterator = values.iterator();
      while (iterator.hasNext()) {
        LogEvent v = iterator.next();
        cnt += 1;
      }

      SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_FORMAT);

      out.collect(Tuple3.of(dateFormat.format(dt), window.getEnd() - window.getStart(), cnt));

      // 此处可以增加ErrorLog 个数的判断逻辑，如果超过阈值，就可以调用报警API，完成报警，大致如下：
      final int maxErrorLogCnt = 5;
      if (cnt > maxErrorLogCnt) {
        // send alert ....
      }
    }
  }


  public static class LogEvent {
    private String eventUUID;
    private Timestamp eventTime;
    private String eventType; // error, info, warn
    private String eventContent = "";

    public LogEvent(String rawString) {

      String[] parts = rawString.split(",");

      if (parts.length != 3) {
        throw new FlinkRuntimeException("invalid data");
      }

      SimpleDateFormat dateFormat = new SimpleDateFormat(TIME_FORMAT);
      try {
        Date parsedDate = dateFormat.parse(parts[0]);
        this.eventTime = new java.sql.Timestamp(parsedDate.getTime());

      } catch (ParseException e) {
        throw new FlinkRuntimeException("invalid data");
      }

      this.eventType = parts[1];
      this.eventContent = parts[2];
      this.eventUUID = UUID.randomUUID().toString();
    }

    public LogEvent(Timestamp eventTime, String eventContent) {
      this.eventTime = eventTime;
      this.eventContent = eventContent;
      this.eventUUID = UUID.randomUUID().toString();
    }

    @Override
    public String toString() {
      return "[" + eventUUID + "] [" + eventTime + "] [" + eventType + "] [" + eventContent + "]";
    }
  }

  public static class Splitter implements MapFunction<String, LogEvent> {

    @Override
    public LogEvent map(String value) throws Exception {

      return new LogEvent(value);
    }
  }

  public static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<LogEvent> {

    // 假设有window = w1 , 它的窗口区间为 [w1.getStart(), w1.getEnd())
    // 如果现在Flink又收到了一个Event，它抽取出的timestamp(t1)，t1 >= w1.getEnd() + maxOutOfOrderness, 则会触发 w1 的窗口计算，并输出。
    // 所以，maxOutOfOrderness 这个时间直接决定了窗口计算结果延迟输出的时间。
    private final long maxOutOfOrderness = 10000; // 10 seconds

    // 同时，你会观察到一个现象，就是如果一直没有收到新的event，可能会导致部分窗口的计算一直不触发

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(LogEvent event, long previousElementTimestamp) {

      currentMaxTimestamp = Math.max(event.eventTime.getTime(), currentMaxTimestamp);
      return event.eventTime.getTime();
    }

    @Override
    public Watermark getCurrentWatermark() {
      // return the watermark as current highest timestamp minus the out-of-orderness bound
      return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
  }
}
