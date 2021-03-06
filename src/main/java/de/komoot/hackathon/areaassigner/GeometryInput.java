/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package de.komoot.hackathon.areaassigner;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vividsolutions.jts.geom.Geometry;

import de.komoot.hackathon.areaassigner.model.PactGeometry;
import de.komoot.hackathon.openstreetmap.GeometryModule;
import de.komoot.hackathon.openstreetmap.JsonGeometryEntity;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class GeometryInput extends MapStub {
	
	// initialize reusable mutable objects
	private final PactRecord outputRecord = new PactRecord();
	private final PactString geometryId = new PactString();
	private final PactGeometry geometry = new PactGeometry();
	private final ObjectMapper mapper;
	
	public GeometryInput() {
		mapper = new ObjectMapper();
		mapper.registerModule(new GeometryModule());
		mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
	}
	
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector)
			throws Exception {
		
		PactString line = record.getField(0, PactString.class);
		JsonGeometryEntity<Geometry> entry;
		
		try {
			entry = mapper.readValue(line.getValue(), JsonGeometryEntity.class);
			
			geometryId.setValue(entry.getId());
			geometry.setValue(entry.getGeometry());
			
			outputRecord.setField(0, geometryId);
			outputRecord.setField(1, geometry);
			
			collector.collect(outputRecord);
			
		} catch (RuntimeException e) {
			throw new RuntimeException("Unable to parse line: " + line + ":", e);
		}
		
		
	}

}