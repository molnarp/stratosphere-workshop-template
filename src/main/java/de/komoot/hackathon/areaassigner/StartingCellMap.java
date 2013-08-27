package de.komoot.hackathon.areaassigner;

import java.util.Set;

import com.vividsolutions.jts.geom.Geometry;

import de.komoot.hackathon.areaassigner.model.PactGeometry;
import de.komoot.hackathon.areaassigner.utils.NewGrid;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class StartingCellMap extends MapStub {

    private final PactRecord outputRecord = new PactRecord();
    private final PactString cellId = new PactString();
    private int zoom;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        zoom = parameters.getInteger("zoom", 12);
    }

    public int getZoom() {
        return zoom;
    }

    public void setZoom(int zoom) {
        this.zoom = zoom;
    }
    
    @Override
    public void map(PactRecord geometryRecord, Collector<PactRecord> out) throws Exception {

        this.outputRecord.setField(1, geometryRecord.getField(0, PactString.class));
        this.outputRecord.setField(2, geometryRecord.getField(1, PactGeometry.class));
        this.outputRecord.setField(3, new PactInteger(zoom));
        Geometry geometry = geometryRecord.getField(1, PactGeometry.class).getGeometry();

        NewGrid grid = new NewGrid(zoom);
        Set<String> cellIds = grid.getCellIds(geometry);
        for (String cellId : cellIds) {
            this.cellId.setValue(cellId);
            this.outputRecord.setField(0, this.cellId);
            out.collect(this.outputRecord);
        }

    }
}
