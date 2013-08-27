package de.komoot.hackathon.areaassigner.utils;

import com.vividsolutions.jts.geom.*;
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
	public void testGetCellIdsPoint() throws Exception {
		Point point = factory.createPoint(new Coordinate(19.01804, 47.48748));
		Set<String> cellIds = Grid.getCellIds(point, 12);

		HashSet<String> expected = new HashSet<>();
		expected.add("2264:1432");

		assertEquals(expected.size(), cellIds.size());
		assertTrue(expected.containsAll(cellIds));
	}

	@Test
	public void testGetCellIdsPolygon() throws Exception {
		Coordinate c = new Coordinate(19.01804, 47.48748);

		Point point = factory.createPoint(c);
		Geometry area = point.buffer(0.03);

		Set<String> cellIds = Grid.getCellIds(area, 12);

		HashSet<String> expected = new HashSet<>();
		expected.add("2264:1431");
		expected.add("2264:1432");
		expected.add("2264:1433");

		assertEquals(expected.size(), cellIds.size());
		assertTrue(expected.containsAll(cellIds));
	}
}
