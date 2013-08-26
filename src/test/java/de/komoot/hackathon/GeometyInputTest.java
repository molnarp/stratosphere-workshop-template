package de.komoot.hackathon;


import org.junit.Assert;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKBReader;

import de.komoot.hackathon.areaassigner.CollectorHelper;
import de.komoot.hackathon.areaassigner.GeometryInputMapper;
import de.komoot.hackathon.areaassigner.model.PactGeometry;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class GeometyInputTest {
	
	private GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel());
	
	@Test
	public void testInput1() {

		CollectorHelper collector = new CollectorHelper();
		PactGeometry expectedGeometry = new PactGeometry(geometryFactory.createPoint(new Coordinate(8.786804700000001,53.0749415)));		
		
		PactRecord input = new PactRecord();
		// Line: point 8.786804700000001,53.0749415
		PactString line = new PactString("{\"id\":\"N125799\",\"geometry\":\"0020000001000010E6402192D810CDAD9E404A8997AEDDCE7D\",\"tags\":{}}");
		input.addField(line);
		
		GeometryInputMapper tested = new GeometryInputMapper();
		
		try {
			tested.map(input, collector);
			
			PactRecord outRecord = collector.get(0);
			
			Assert.assertEquals("N125799", outRecord.getField(0, PactString.class).getValue());
			PactGeometry outGeometry = outRecord.getField(1, PactGeometry.class);
			
			Assert.assertTrue(expectedGeometry.equals(outGeometry));
						
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		
	}	
}
