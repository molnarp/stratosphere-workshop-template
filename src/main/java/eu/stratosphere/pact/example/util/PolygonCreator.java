/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.stratosphere.pact.example.util;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBWriter;

/**
 *
 * @author molnarp
 */
public class PolygonCreator {
 
    public static void main(String[] args) throws Exception {
        GeometryFactory gf = new GeometryFactory();
        Point p = gf.createPoint(new Coordinate(0,0));
        
        WKBWriter wr = new WKBWriter();
        System.out.println(String.format("Point: %1$s", WKBWriter.toHex(wr.write(p))));
        
        
        LinearRing lr = gf.createLinearRing(new Coordinate[] { new Coordinate(-1, -1), new Coordinate(-1, 1), new Coordinate(1,1), new Coordinate(1,-1), new Coordinate(-1, -1)});
        Polygon g = gf.createPolygon(lr, null);
        
        wr = new WKBWriter();
        System.out.println(String.format("Polygon: %1$s", WKBWriter.toHex(wr.write(g))));
        
    }
}
