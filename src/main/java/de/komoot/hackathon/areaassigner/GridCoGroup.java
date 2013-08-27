package de.komoot.hackathon.areaassigner;

import java.util.Iterator;
import java.util.Set;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import de.komoot.hackathon.areaassigner.model.PactGeometry;
import de.komoot.hackathon.areaassigner.utils.NewGrid;
import eu.stratosphere.nephele.configuration.Configuration;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class GridCoGroup extends CoGroupStub {

  private int threshold = 50;
  private final PactRecord outputRecord = new PactRecord();
  private PactRecord geomRecord;
  private final GeometryFactory factory = new GeometryFactory();
  private final PactString id = new PactString();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        threshold = parameters.getInteger("threshold", 50);
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }
  
  
  
  @Override
  public void coGroup(Iterator<PactRecord> counterRecord, Iterator<PactRecord> geometryRecords,
      Collector<PactRecord> out) {
    PactRecord counterRec = counterRecord.next();
    int counter = counterRec.getField(1, PactInteger.class).getValue();
    if (counter < threshold) {
      while (geometryRecords.hasNext()) {
        out.collect(geometryRecords.next());
      }
      return;  
    }
    
    String cellId = counterRec.getField(0, PactString.class).getValue();
    String[] coordinates = cellId.split(",");
    double minX = Double.parseDouble(coordinates[0]);
    double maxX = Double.parseDouble(coordinates[1]);
    double minY = Double.parseDouble(coordinates[2]);
    double maxY = Double.parseDouble(coordinates[3]);
    int zoom = (int) Math.round(Math.log(360 / (maxX - minX)) / Math.log(2));
    int newZoom = zoom + (int) (Math.log10(counter));
    
    Geometry cell = factory.toGeometry(new Envelope(minX, maxX, minY, maxY));
    NewGrid grid = new NewGrid(newZoom, cell);
    
    while (geometryRecords.hasNext()) {
      geomRecord = geometryRecords.next();
      Geometry geometry = geomRecord.getField(2, PactGeometry.class).getGeometry();
      Set<String> cellIds = grid.getCellIds(geometry);
      outputRecord.setField(1, geomRecord.getField(1, PactString.class));
      outputRecord.setField(2, geomRecord.getField(2, PactGeometry.class));
      for(String cId : cellIds) {
        this.id.setValue(cId);
        this.outputRecord.setField(0, this.id);
        out.collect(this.outputRecord);
      }
    }
  }

}
