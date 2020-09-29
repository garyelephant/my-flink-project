package myflink;

import myflink.utils.ListTimedIterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WindowCustomTrigger {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    List<Integer> ints = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      ints.add(i);
    }

    Iterator<Integer> it = new ListTimedIterator(ints, 500);

    DataStream<Integer> intStream = env.fromCollection(it, Integer.class);

    // TODO: custom trigger + TimeWindow

    env.execute("WindowCustomTrigger");
  }
}
