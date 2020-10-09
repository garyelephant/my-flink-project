package myflink;

import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * A trigger combined ContinuousEventTimeTrigger with CountTrigger
 * */
public class CountAndContinuousEventTimeTrigger<W extends Window> extends Trigger<Object, W> {

  private static final long serialVersionUID = 1L;

  private Trigger<Object, W> countTrigger;

  private Trigger<Object, W> continuousEventTimeTrigger;

  /**
   * @param interval in mills
   * @param maxCount
   * */
  private CountAndContinuousEventTimeTrigger(Time interval, long maxCount) {

    this.continuousEventTimeTrigger = ContinuousEventTimeTrigger.of(interval);
    this.countTrigger = CountTrigger.of(maxCount);
  }

  @Override
  public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {

    TriggerResult triggerResult1 = continuousEventTimeTrigger.onElement(element, timestamp, window, ctx);
    TriggerResult triggerResult2 = countTrigger.onElement(element, timestamp, window, ctx);
    return getFinalTriggerResult(triggerResult1, triggerResult2);
  }

  @Override
  public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {

    TriggerResult triggerResult1 = continuousEventTimeTrigger.onProcessingTime(time, window, ctx);
    TriggerResult triggerResult2 = countTrigger.onProcessingTime(time, window, ctx);
    return getFinalTriggerResult(triggerResult1, triggerResult2);
  }

  @Override
  public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {

    TriggerResult triggerResult1 = continuousEventTimeTrigger.onEventTime(time, window, ctx);
    TriggerResult triggerResult2 = countTrigger.onEventTime(time, window, ctx);
    return getFinalTriggerResult(triggerResult1, triggerResult2);
  }

  @Override
  public void clear(W window, TriggerContext ctx) throws Exception {
    continuousEventTimeTrigger.clear(window, ctx);
    countTrigger.clear(window, ctx);
  }

  @Override
  public boolean canMerge() {
    return continuousEventTimeTrigger.canMerge() && countTrigger.canMerge();
  }

  @Override
  public void onMerge(W window, OnMergeContext ctx) throws Exception {
    continuousEventTimeTrigger.onMerge(window, ctx);
    countTrigger.onMerge(window, ctx);
  }

  @Override
  public String toString() {
    return "CountAndContinuousEventTimeTrigger(" + continuousEventTimeTrigger.toString() + "; " + countTrigger.toString() + ")";
  }

  public static <W extends Window> CountAndContinuousEventTimeTrigger<W> of(Time interval, long maxCount) {
    return new CountAndContinuousEventTimeTrigger(interval, maxCount);
  }

  /**
   * This is equivalent to
   * {@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger}
   */
  public static <W extends Window> CountAndContinuousEventTimeTrigger<W> defaultEventTimeTrigger() {
    return CountAndContinuousEventTimeTrigger.of(Time.days(Long.MAX_VALUE), Long.MAX_VALUE);
  }

  /**
   * This is equivalent to
   * {@link CountTrigger}
   * */
  public static <W extends Window> CountAndContinuousEventTimeTrigger<W> onlyCountTrigger(long maxCount) {
    return CountAndContinuousEventTimeTrigger.of(Time.days(Long.MAX_VALUE), maxCount);
  }

  /**
   * This is equivalent to
   * {@link ContinuousEventTimeTrigger}
   * */
  public static <W extends Window> CountAndContinuousEventTimeTrigger<W> onlyContinuousEventTimeTrigger(Time interval) {
    return CountAndContinuousEventTimeTrigger.of(interval, Long.MAX_VALUE);
  }

  /**
   * 由于ContinuousEventTimeTrigger与CountTrigger只会返回TriggerResult.CONTINUE, TriggerResult.FIRE这两种，
   * 所以这里我们只需要比较这两种即可。
   * */
  private TriggerResult getFinalTriggerResult(TriggerResult triggerResult1, TriggerResult triggerResult2) {

    if (triggerResult1 == TriggerResult.FIRE || triggerResult2 == TriggerResult.FIRE) {
      return TriggerResult.FIRE;
    } else {
      return TriggerResult.CONTINUE;
    }
  }
}
