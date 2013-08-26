package de.komoot.hackathon.areaassigner.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.vividsolutions.jts.geom.Envelope;

import eu.stratosphere.pact.common.type.Value;

public class PactEnvelope implements Value {

  private Envelope envelope;
  
  public PactEnvelope(Envelope envelope) {
    this.envelope = envelope;
  }

  public void setValue(Envelope envelope) {
    this.envelope = envelope;
  }
  
  public Envelope getValue() {
    return envelope;
  }
  
  public Envelope getEnvelope() {
    return envelope;
  }
  
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeDouble(envelope.getMinX());
    dataOutput.writeDouble(envelope.getMaxX());
    dataOutput.writeDouble(envelope.getMinY());
    dataOutput.writeDouble(envelope.getMaxY());
  }

  @Override
  public void read(DataInput dataInput) throws IOException {
    double maxY = dataInput.readDouble();
    double minY = dataInput.readDouble();
    double maxX = dataInput.readDouble();
    double minX = dataInput.readDouble();
    envelope = new Envelope(minX, maxX, minY, maxY);
  }
}
