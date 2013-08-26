package de.komoot.hackathon.areaassigner;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import de.komoot.hackathon.areaassigner.model.PactGeometry;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;

@ConstantFields(fields = {0, 1})
@OutCardBounds(lowerBound = 1, upperBound = 1)
public class AreaBBox extends MapStub {

  @Override
  public void map(PactRecord areaRecord, Collector<PactRecord> out) throws Exception {
    PactGeometry geometry = areaRecord.getField(1, PactGeometry.class);
    areaRecord.addField(geometry.getGeometry().getEnvelopeInternal());
    out.collect(areaRecord);
  }
  
}