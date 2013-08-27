package de.komoot.hackathon.areaassigner;

import java.util.Set;

import com.vividsolutions.jts.geom.Geometry;

import de.komoot.hackathon.areaassigner.model.PactGeometry;
import de.komoot.hackathon.areaassigner.utils.NewGrid;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class StartingCellMap extends MapStub {

  private final PactRecord outputRecord = new PactRecord();
  private final PactString cellId = new PactString();
  
  @Override
  public void map(PactRecord geometryRecord, Collector<PactRecord> out) throws Exception {
    
    this.outputRecord.setField(1, geometryRecord.getField(0, PactString.class));
    this.outputRecord.setField(2, geometryRecord.getField(1, PactGeometry.class));
    
    Geometry geometry = geometryRecord.getField(1, PactGeometry.class).getGeometry();

    NewGrid grid = new NewGrid(4);
    Set<String> cellIds = grid.getCellIds(geometry);
    for(String cellId : cellIds) {
      this.cellId.setValue(cellId);
      this.outputRecord.setField(0, this.cellId);
      out.collect(this.outputRecord);
    }

  }

}
