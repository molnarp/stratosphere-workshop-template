package de.komoot.hackathon.areaassigner.utils;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import java.util.HashSet;
import java.util.Set;

public class NewGrid {
  
  public NewGrid(int zoom) {
    this.zoom = zoom;
  }
  
	private int zoom;

	public Set<String> getCellIds(Geometry geom) {
		Envelope envelope = geom.getEnvelopeInternal();

		int xMin = getX(envelope.getMinX());
		int yMax = getY(envelope.getMinY());

		int xMax = getX(envelope.getMaxX());
		int yMin = getY(envelope.getMaxY());

		HashSet<String> cellIds = new HashSet<>();

		for(int x = xMin; x <= xMax; x++) {
			for(int y = yMin; y <= yMax; y++) {
				cellIds.add(getTileId(x, y));
			}
		}
		return cellIds;
	}

	private String getTileId(final int x, final int y) {
		return ((double) x / (1 << zoom) * 360 - 180.0) + "," + ((double) (x + 1) / (1 << zoom) * 360 - 180.0) + "," +
		    ((double) y / (1 << zoom) * 180 - 90.0) + "," + ((double) (y + 1) / (1 << zoom) * 180 - 90.0);
	}

	private int getX(double lon) {
		return (int) Math.floor((lon + 180) / 360 * (1 << zoom));
	}

  private int getY(double lat) {
    return (int) Math.floor((lat + 90) / 180 * (1 << zoom));
  }
	
}
