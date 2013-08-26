package de.komoot.hackathon.areaassigner;

import com.vividsolutions.jts.geom.Geometry;
import de.komoot.hackathon.areaassigner.model.PactGeometry;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Mach the same callid-s point and poligons and see the really boundings.
 * 
 * @author lukacsg
 */
//@ConstantFields(fields = {})
//@OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
public class IdMatcher extends MatchStub {
    private final PactRecord outputRecord = new PactRecord();
    private Geometry point;
    private Geometry geom;
    
    @Override
    public void match(PactRecord pr, PactRecord pr1, Collector<PactRecord> clctr) throws Exception {
        point = (Geometry) (pr.getField(2, PactGeometry.class).getGeometry()).clone();
        geom = (Geometry) (pr1.getField(2, PactGeometry.class).getGeometry()).clone();
        if(geom.intersects(point)) {
            outputRecord.setField(0, new PactString(pr.getField(1, PactString.class).getValue()));
            outputRecord.setField(1, new PactString(pr1.getField(1, PactString.class).getValue()));
            clctr.collect(outputRecord);
        }
    }

}