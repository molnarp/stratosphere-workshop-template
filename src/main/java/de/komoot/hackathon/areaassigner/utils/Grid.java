package de.komoot.hackathon.areaassigner.utils;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import java.util.HashSet;
import java.util.Set;

/**
 * @author christoph
 * @date 26.08.13
 */
public class Grid {

	public static Set<String> getCellIds(Geometry geom, int zoom) {
		Envelope envelope = geom.getEnvelopeInternal();

		int xMin = getX(envelope.getMinX(), zoom);
		int yMax = getY(envelope.getMinY(), zoom);

		int xMax = getX(envelope.getMaxX(), zoom);
		int yMin = getY(envelope.getMaxY(), zoom);

		HashSet<String> cellIds = new HashSet<>();

		for(int x = xMin; x <= xMax; x++) {
			for(int y = yMin; y <= yMax; y++) {
				cellIds.add(getTileId(x, y, zoom));
			}
		}
		return cellIds;
	}

	private static String getTileId(final int x, final int y, int zoom) {
		return String.format("%d:%d", x, y);
	}

	private static int getX(double lon, int zoom) {
		return (int) Math.floor((lon + 180) / 360 * (1 << zoom));
	}

	private static int getY(double lat, int zoom) {
		return (int) Math.floor((1 - Math.log(Math.tan(Math.toRadians(lat)) + 1 / Math.cos(Math.toRadians(lat))) / Math.PI) / 2 * (1 << zoom));
	}
}
