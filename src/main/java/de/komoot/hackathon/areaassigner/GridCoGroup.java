package de.komoot.hackathon.areaassigner;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;

public class GridCoGroup extends CoGroupStub {

  @Override
  public void coGroup(Iterator<PactRecord> arg0, Iterator<PactRecord> arg1,
      Collector<PactRecord> arg2) {
    // TODO Auto-generated method stub

  }

}
