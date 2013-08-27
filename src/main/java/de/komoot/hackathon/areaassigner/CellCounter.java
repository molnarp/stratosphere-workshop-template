package de.komoot.hackathon.areaassigner;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class CellCounter extends ReduceStub {

  private final PactRecord outputRecord = new PactRecord();
  private final PactInteger integer = new PactInteger();
  
  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
      throws Exception {
    int counter = 0;
    while (records.hasNext()) {
      outputRecord.setField(0, records.next().getField(0, PactString.class));
      ++counter;
    }
    
    this.integer.setValue(counter);
    outputRecord.setField(1, this.integer);
    out.collect(outputRecord);
  }

}
