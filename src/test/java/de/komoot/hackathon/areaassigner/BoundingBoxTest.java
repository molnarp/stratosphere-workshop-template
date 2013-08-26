package de.komoot.hackathon.areaassigner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import de.komoot.hackathon.areaassigner.model.PactGeometry;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class BoundingBoxTest {

  @Test
  public void testMap() {
    GeometryFactory factory = new GeometryFactory();
    Geometry geometry = factory.createPoint(new Coordinate(10, 20));
    Geometry geometry2 = factory.createPoint(new Coordinate(10, 30));
    BoundingBox bb = new BoundingBox();
    PactRecord areaRecord = new PactRecord();
    areaRecord.addField(new PactString("P-01"));
    areaRecord.addField(new PactGeometry(geometry));
    PactRecord areaRecord2 = new PactRecord();
    areaRecord2.addField(new PactString("P-02"));
    areaRecord2.addField(new PactGeometry(geometry2));
    CollectorHelper collector = new CollectorHelper();
    
    try {
      bb.map(areaRecord, collector);
      bb.map(areaRecord2, collector);
    } catch (Exception e) {
      fail();
    }
    
    assertEquals(2, collector.size());
    assertEquals("P-01", collector.get(0).getField(0, PactString.class).getValue());
    assertEquals(3, collector.get(0).getNumFields());
    
  }

}
