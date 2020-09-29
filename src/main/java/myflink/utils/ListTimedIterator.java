package myflink.utils;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class ListTimedIterator<VType> implements Iterator<VType>, Serializable {

  private List<VType> ints;
  private int currentIndex;
  private long sleepMills;

  public ListTimedIterator(List<VType> ints, long sleepMills) {
    this.ints = ints;
    this.currentIndex = -1;
    this.sleepMills = sleepMills;
  }

  @Override
  public boolean hasNext() {

    currentIndex++;

    return (currentIndex < ints.size());
  }

  @Override
  public VType next() {

    try {
      Thread.sleep(this.sleepMills);
    } catch (InterruptedException e) {
      System.exit(-1);
    }

    return ints.get(currentIndex);
  }
}
