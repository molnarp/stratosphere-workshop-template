package de.komoot.hackathon.areaassigner.model;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import eu.stratosphere.pact.common.type.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author christoph
 * @date 26.08.13
 */
public class PactGeometry implements Value {
	private Geometry geometry;
	
	public PactGeometry() {
	}
	public PactGeometry(Geometry geometry) {
		this.geometry = geometry;
	}

	public void setValue(Geometry geometry) {
		this.geometry = geometry;
	}
	
	public Geometry getValue() {
		return geometry;
	}
	
	public Geometry getGeometry() {
		return geometry;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WKBWriter writer = new WKBWriter();

		byte[] bytes = writer.write(this.geometry);
		dataOutput.writeInt(bytes.length);
		dataOutput.write(bytes);
	}

	@Override
	public void read(DataInput dataInput) throws IOException {
		WKBReader reader = new WKBReader();

		int size = dataInput.readInt();
		byte[] bytes = new byte[size];
		dataInput.readFully(bytes);

		try {
			this.geometry = reader.read(bytes);
		} catch(ParseException e) {
			throw new RuntimeException(e);
		}
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((geometry == null) ? 0 : geometry.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PactGeometry other = (PactGeometry) obj;
		if (geometry == null) {
			if (other.geometry != null)
				return false;
		} else if (!geometry.equals(other.geometry))
			return false;
		return true;
	}
	
	
}
