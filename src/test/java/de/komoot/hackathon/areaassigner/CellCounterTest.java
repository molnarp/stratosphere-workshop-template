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
public class CellCounterTest {
 
    @Test public void testCellCounter1() throws Exception {
        PactRecord r1 = new PactRecord();
        r1.setField(0, new PactString("N1"));
        PactRecord r2 = new PactRecord();
        r2.setField(0, new PactString("N1"));
        
        List<PactRecord> input = Arrays.asList(r1, r2);
        
        CellCounter cc = new CellCounter();
        CollectorHelper ch = new CollectorHelper();
        cc.reduce(input.iterator(), ch);
        
        Assert.assertEquals(1, ch.size());
        PactRecord output1 = ch.get(0);
        
        Assert.assertEquals("N1", output1.getField(0, PactString.class).getValue());
        Assert.assertEquals(2, output1.getField(1, PactInteger.class).getValue());        
    }
}
