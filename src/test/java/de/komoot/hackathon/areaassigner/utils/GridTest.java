package de.komoot.hackathon.areaassigner.utils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import org.junit.*;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author christoph
 * @date 26.08.13
 */
public class GridTest {
	private GeometryFactory factory;

	@Before
	public void setUp() throws Exception {
		factory = new GeometryFactory(new PrecisionModel(), 4326);
	}

	@Test
	public void testGetCellIds() throws Exception {
		Point point = factory.createPoint(new Coordinate(0.01, 0.01));
		Set<String> cellIds = Grid.getCellIds(point);

		HashSet<String> expected = new HashSet<>();
		expected.add("0:0");

		assertEquals(expected.size(), cellIds.size());
		//assertTrue(expected.containsAll(cellIds));
	}
}
