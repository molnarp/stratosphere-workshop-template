package de.komoot.hackathon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import de.komoot.hackathon.areaassigner.model.PactGeometry;

public class PactGeometryTest {

	private GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel());
	
	@Test public void testSerialization() {
		
		try {
			Point p = geometryFactory.createPoint(new Coordinate(1,1));
			PactGeometry pgIn = new PactGeometry(p);
			
			ByteArrayOutputStream bdos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(bdos);
			
			pgIn.write(dos);
			dos.flush();
			dos.close();
			
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bdos.toByteArray()));
			
			PactGeometry pgOut = new PactGeometry();
			pgOut.read(dis);
			
			Assert.assertTrue(pgIn.equals(pgOut));
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
