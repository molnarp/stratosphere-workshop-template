package de.komoot.hackathon.areaassigner;

import de.komoot.hackathon.areaassigner.model.PactGeometry;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 *Separator map. Separate area from geometry.
 * 
 * @author lukacsg
 */
public class AreaSeparatorMap extends MapStub {
    private final PactRecord output = new PactRecord();

    @Override
    public void map(PactRecord pr, Collector<PactRecord> clctr) throws Exception {
        if(!pr.getField(1, PactString.class).getValue().startsWith("A")) return;
        output.setField(0, new PactString(pr.getField(0, PactString.class).getValue()));
        output.setField(1, new PactString(pr.getField(1, PactString.class).getValue()));
        output.setField(2, new PactGeometry(pr.getField(2, PactGeometry.class).getValue()));
        clctr.collect(output);
    }
}
