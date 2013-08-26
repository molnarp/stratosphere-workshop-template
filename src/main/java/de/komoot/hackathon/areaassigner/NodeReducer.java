package de.komoot.hackathon.areaassigner;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/*
 * Reduce the same node and make the area list to the distinct node.
 * 
 * @author lukacsg
 */
public class NodeReducer extends ReduceStub{
    private final PactRecord outputRecord = new PactRecord();
    private PactRecord tmp;
    private StringBuffer buffer;
    private PactString id;
    
    @Override
    public void reduce(Iterator<PactRecord> itrtr, Collector<PactRecord> clctr) throws Exception {
        tmp = itrtr.next();
        id = new PactString(tmp.getField(0, PactString.class).getValue());
        buffer = new StringBuffer();
        buffer.append(tmp.getField(1, PactString.class).getValue());
        buffer.append(",");
        while (itrtr.hasNext()) {
            buffer.append(itrtr.next().getField(1, PactString.class).getValue());
            buffer.append(",");
        }
        buffer.deleteCharAt(buffer.length()-1);
        outputRecord.setField(0, id);
        outputRecord.setField(1, new PactString(buffer.toString()));
        clctr.collect(outputRecord);
    }
}