package de.komoot.hackathon.areaassigner;

import com.vividsolutions.jts.geom.Point;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;

@ConstantFields(fields = {0, 1})
@OutCardBounds(lowerBound = 1, upperBound = 1)
public class NodeBBox extends MapStub {

  @Override
  public void map(PactRecord pointRecord, Collector<PactRecord> out) throws Exception {
    Point point = pointRecord.getField(1, Point.class);
    pointRecord.addField(point.getBoundary().getEnvelopeInternal());
    out.collect(pointRecord);
  }

}