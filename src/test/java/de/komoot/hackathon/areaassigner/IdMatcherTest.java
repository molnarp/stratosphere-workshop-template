package de.komoot.hackathon.areaassigner;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import de.komoot.hackathon.areaassigner.model.PactGeometry;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author lukacsg
 */
public class IdMatcherTest {
    
    public IdMatcherTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }


    /**
     * Test of match method, of class IdMatcher.
     */
    @Test
    public void testMatch() throws Exception {
        GeometryFactory factory = new GeometryFactory(new PrecisionModel());
        PactRecord pr = new PactRecord();
        pr.setField(0, new PactString("sameid"));
        pr.setField(1, new PactString("testgoodpoint"));
        pr.setField(2, new PactGeometry(factory.createPoint(new Coordinate(1, 1))));
        PactRecord pr1 = new PactRecord();
        pr1.setField(0, new PactString("sameid"));
        pr1.setField(1, new PactString("testgoodgeomerty"));
        pr1.setField(2, new PactGeometry(factory.createPoint(new Coordinate(1, 1))));
        Collector<PactRecord> clctr = new CollectorHelper();
        IdMatcher instance = new IdMatcher();
        instance.match(pr, pr1, clctr);
        assertEquals(((CollectorHelper)clctr).size(), 1);
        PactRecord tmp = ((CollectorHelper)clctr).get(0);
        assertEquals(tmp.getField(0, PactString.class).getValue(), "testgoodpoint");
        assertEquals(tmp.getField(1, PactString.class).getValue(), "testgoodgeomerty");
    }

    /**
     * Test of match method, of class IdMatcher.
     */
    @Test
    public void testMatch2() throws Exception {
        GeometryFactory factory = new GeometryFactory(new PrecisionModel());
        PactRecord pr = new PactRecord();
        pr.setField(0, new PactString("sameid"));
        pr.setField(1, new PactString("testbadpoint"));
        pr.setField(2, new PactGeometry(factory.createPoint(new Coordinate(1, 1))));
        PactRecord pr1 = new PactRecord();
        pr1.setField(0, new PactString("sameid"));
        pr1.setField(1, new PactString("testbadgeomerty"));
        pr1.setField(2, new PactGeometry(factory.createPoint(new Coordinate(2, 2))));
        Collector<PactRecord> clctr = new CollectorHelper();
        IdMatcher instance = new IdMatcher();
        instance.match(pr, pr1, clctr);
        assertEquals(((CollectorHelper)clctr).size(), 0);
    }
}