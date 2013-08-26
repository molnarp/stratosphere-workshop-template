package hu.sztaki.ilab.cumulonimbus.helper;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;

public class CollectorHelper implements Collector<PactRecord> {

  private List<PactRecord> list_ = new ArrayList<PactRecord>();
  
  @Override
  public void close() {
  }
  
  @Override
  public void collect(PactRecord arg0) {
    list_.add(arg0.createCopy());
  }

  public PactRecord get(int index) {
    return list_.get(index);
  }
  
  public int size() {
    return list_.size();
  }
}
