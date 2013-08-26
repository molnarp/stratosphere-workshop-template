package de.komoot.hackathon.openstreetmap;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import java.util.Map;

/** @author jan */
public class OsmNodeImpl implements OsmNode {

	private final static GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);

	private final long id;

	private final Coordinate coordinate;

	public OsmNodeImpl(long osmId, Coordinate coordinate) {
		this.id = osmId;
		this.coordinate = coordinate;
	}

	@Override
	public Coordinate getCoordinate() {
		return coordinate;
	}

	@Override
	public long getOsmId() {
		return id;
	}

	@Override
	public String getId() {
		return "N" + getOsmId();
	}

	@Override
	public Point getGeometry() {
		return gf.createPoint(coordinate);
	}

	@Override
	public Map<String, String> getTags() {
		return null;
	}
}
