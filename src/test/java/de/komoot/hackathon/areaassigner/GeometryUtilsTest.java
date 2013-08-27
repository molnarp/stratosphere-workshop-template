/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.komoot.hackathon.areaassigner;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import de.komoot.hackathon.areaassigner.utils.GeometryUtils;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author molnarp
 */
public class GeometryUtilsTest {

    private GeometryFactory gf = new GeometryFactory();
    
    @Test public void testSplitLine() {
        LineString exp1 = gf.createLineString(new Coordinate[] { new Coordinate(0,0), new Coordinate(1,0)});
        LineString exp2 = gf.createLineString(new Coordinate[] { new Coordinate(1,0), new Coordinate(2,0)});
        
        LineString line = gf.createLineString(new Coordinate[] { new Coordinate(0,0), new Coordinate(1,0), new Coordinate(2,0)});
        List<LineString> parts = GeometryUtils.splitLine(line);
        
        Assert.assertEquals(2, parts.size());
        Assert.assertTrue(exp1.equals(parts.get(0)));
        Assert.assertTrue(exp2.equals(parts.get(1)));
    }
}
