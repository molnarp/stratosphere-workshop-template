package de.komoot.hackathon.areaassigner;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import de.komoot.hackathon.areaassigner.model.PactEnvelope;
import de.komoot.hackathon.areaassigner.model.PactGeometry;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import org.junit.*;

/**
 * @author christoph
 * @date 26.08.13
 */
public class CellIdTest {
	private CellId cut;
	private GeometryFactory factory = new GeometryFactory(new PrecisionModel(), 4326);

	@Before
	public void setUp() throws Exception {
		cut = new CellId();
	}

	public void testMap() throws Exception {
		PactRecord pactRecord = new PactRecord();

		PactInteger nodeId = new PactInteger(1234);
		Coordinate coordinate = new Coordinate(10, 47, 1200);
		Point point = factory.createPoint(coordinate);

		pactRecord.setField(0, nodeId);
		pactRecord.setField(1, new PactGeometry(point));
		pactRecord.setField(2, new PactEnvelope(point.getEnvelopeInternal()));

		cut.map(pactRecord, );

		cut.map();
	}
}
