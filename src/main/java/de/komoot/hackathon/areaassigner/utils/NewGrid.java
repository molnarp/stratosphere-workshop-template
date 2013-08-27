package de.komoot.hackathon.areaassigner.utils;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import java.util.HashSet;
import java.util.Set;

public class NewGrid {
  
  public NewGrid(int zoom) {
    this.zoom = zoom;
  }
  
  public NewGrid (int zoom, Geometry geometry) {
    this(zoom);
    this.geometry = geometry;
  }
  
	private int zoom;
	private Geometry geometry;
	
	public Set<String> getCellIds(Geometry geom) {
	  Envelope envelope;
		if (geometry == null) {
		  envelope = geom.getEnvelopeInternal();
		} else {
		  envelope = geom.intersection(geometry).getEnvelopeInternal();
		}

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
	  double minX = tile2lon(x);
    double maxX = tile2lon(x + 1);
    double minY = tile2lat(y);
    double maxY = tile2lat(y + 1);
    
		return minX + "," + maxX + "," + minY + "," + maxY;
	}

	private int getX(double lon) {
		return (int) Math.floor((lon + 180) / 360 * (1 << zoom));
	}

  private int getY(double lat) {
    return (int) Math.floor((1 - Math.log(Math.tan(Math.toRadians(lat)) + 1 / Math.cos(Math.toRadians(lat))) / Math.PI) / 2 * (1 << zoom));
  }
  
  private double tile2lon(int x) {
    return x / Math.pow(2.0, zoom) * 360.0 - 180;
  }

  private double tile2lat(int y) {
    double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, zoom);
    return Math.toDegrees(Math.atan(Math.sinh(n)));
  }
}
