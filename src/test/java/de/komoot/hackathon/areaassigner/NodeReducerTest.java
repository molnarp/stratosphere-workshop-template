/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.komoot.hackathon.areaassigner;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
public class NodeReducerTest {
    
    public NodeReducerTest() {
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
     * Test of reduce method, of class NodeReducer.
     */
    @Test
    public void testReduce() throws Exception {
        List<PactRecord> list = new ArrayList<PactRecord>();
        list.add(new PactRecord(new PactString("id1"), new PactString("1")));
        list.add(new PactRecord(new PactString("id1"), new PactString("2")));
        list.add(new PactRecord(new PactString("id1"), new PactString("3")));
        Iterator<PactRecord> itrtr = list.iterator();
        Collector<PactRecord> clctr = new CollectorHelper();
        NodeReducer instance = new NodeReducer();
        instance.reduce(itrtr, clctr);
        assertEquals(1, ((CollectorHelper)clctr).size());
        PactRecord tmp = ((CollectorHelper)clctr).get(0);
        assertEquals(tmp.getField(0, PactString.class).getValue(), "id1");
        assertEquals(tmp.getField(1, PactString.class).getValue(), "1,2,3");
    }

    /**
     * Test of reduce method, of class NodeReducer.
     */
    @Test
    public void testReduce2() throws Exception {
        List<PactRecord> list = new ArrayList<PactRecord>();
        list.add(new PactRecord(new PactString("id2"), new PactString("1")));
        list.add(new PactRecord(new PactString("id2"), new PactString("2")));
        Iterator<PactRecord> itrtr = list.iterator();
        Collector<PactRecord> clctr = new CollectorHelper();
        NodeReducer instance = new NodeReducer();
        instance.reduce(itrtr, clctr);
        assertEquals(1, ((CollectorHelper)clctr).size());
        PactRecord tmp = ((CollectorHelper)clctr).get(0);
        assertEquals(tmp.getField(0, PactString.class).getValue(), "id2");
        assertEquals(tmp.getField(1, PactString.class).getValue(), "1,2");
    }

    /**
     * Test of reduce method, of class NodeReducer.
     */
    @Test
    public void testReduce3() throws Exception {
        List<PactRecord> list = new ArrayList<PactRecord>();
        list.add(new PactRecord(new PactString("id3"), new PactString("1")));
        Iterator<PactRecord> itrtr = list.iterator();
        Collector<PactRecord> clctr = new CollectorHelper();
        NodeReducer instance = new NodeReducer();
        instance.reduce(itrtr, clctr);
        assertEquals(1, ((CollectorHelper)clctr).size());
        PactRecord tmp = ((CollectorHelper)clctr).get(0);
        assertEquals(tmp.getField(0, PactString.class).getValue(), "id3");
        assertEquals(tmp.getField(1, PactString.class).getValue(), "1");
    }
}