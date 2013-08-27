/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.komoot.hackathon.areaassigner;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author molnarp
 */
public class GridCoGroupTest {
    @Test public void testGridCoGroupUnderTreshold() {
        PactRecord c1 = new PactRecord();
        c1.setField(0, new PactString("N1"));
        c1.setField(1, new PactInteger(2));
        List<PactRecord> countersIn = Arrays.asList(c1);
        
        PactRecord r1 = new PactRecord();
        r1.setField(0, new PactString("N1"));
        PactRecord r2 = new PactRecord();
        r2.setField(0, new PactString("N1"));
        
        List<PactRecord> nodesIn = Arrays.asList(r1, r2);

        GridCoGroup gcg = new GridCoGroup();
        gcg.setThreshold(3);
        
        CollectorHelper ch = new CollectorHelper();
        
        gcg.coGroup(countersIn.iterator(), nodesIn.iterator(), ch);
        
        Assert.assertEquals(2, ch.size());
    }
}
