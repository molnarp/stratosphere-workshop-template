package de.komoot.hackathon.areaassigner;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKBWriter;
import de.komoot.hackathon.areaassigner.model.PactEnvelope;
import de.komoot.hackathon.areaassigner.model.PactGeometry;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * @author christoph
 * @date 26.08.13
 */
public class CellIdTest {
	private final static Logger LOGGER = LoggerFactory.getLogger(CellIdTest.class);
	private CellId cut;
	private GeometryFactory factory = new GeometryFactory(new PrecisionModel(), 4326);

	@Before
	public void setUp() throws Exception {
		cut = new CellId();
	}

	//@Test
	public void testMap() throws Exception {
        
		PactRecord pactRecord = new PactRecord();

		PactString nodeId = new PactString("N1234");
		Coordinate coordinate = new Coordinate(10, 47, 1200);

		Point point = factory.createPoint(coordinate);
		PactGeometry geometry = new PactGeometry(point);
		PactEnvelope envelope = new PactEnvelope(point.getEnvelopeInternal());

		pactRecord.setField(0, nodeId);
		pactRecord.setField(1, geometry);
		pactRecord.setField(2, envelope);

		CollectorHelper collector = new CollectorHelper();

		cut.map(pactRecord, collector);

		PactRecord actual = collector.get(0);
		//		PactString cellId = actual.getField(0, PactString.class);
		//		assertEquals("9.0:46.0", cellId.getValue());
		assertEquals(nodeId.getValue(), actual.getField(1, PactString.class).getValue());
		assertTrue(geometry.getGeometry().equalsExact(actual.getField(2, PactGeometry.class).getGeometry(), 0.1));
	}

	@Test
	public void testName() throws Exception {
		Coordinate c = new Coordinate(0, 0);
		Envelope e = new Envelope(c);
		e.expandBy(1);
		Geometry area = factory.toGeometry(e);
		Geometry point = factory.createPoint(c);

		WKBWriter writer = new WKBWriter();
		byte[] write = writer.write(area);
		byte[] write1 = writer.write(point);

		LOGGER.info("");
	}
}
