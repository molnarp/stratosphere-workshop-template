/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.komoot.hackathon.areaassigner.utils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author molnarp
 */
abstract public class GeometryUtils {
    public static List<LineString> splitLine(LineString input) {
        GeometryFactory gf = new GeometryFactory();
        List<LineString> retval = new ArrayList<>();
        
        Coordinate c1 = null;
        for (Coordinate c : input.getCoordinates()) {
            if (c1 == null) {
                c1 = c;
                continue;
            }
            
            retval.add(gf.createLineString(new Coordinate[] { c1, c }));
            
            c1 = c;
        }
        
        return retval;
    }
}
