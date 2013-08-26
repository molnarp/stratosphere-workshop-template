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

import de.komoot.hackathon.Grid;
import de.komoot.hackathon.areaassigner.model.PactGeometry;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

import java.util.List;

public class AreaCellId extends MapStub {
	// initialize reusable mutable objects
	private final PactRecord outputRecord = new PactRecord();
	private final PactString cellId = new PactString();
	public Grid grid = new Grid(0.1);

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		PactInteger areaId = record.getField(0, PactInteger.class);
		PactGeometry geometry = record.getField(1, PactGeometry.class);

		this.outputRecord.setField(1, areaId);
		this.outputRecord.setField(2, geometry);

		// tokenize the line
		List<String> cellIds = grid.getIdsForGeometry(geometry.getGeometry());
		for(String cellId : cellIds) {
			this.cellId.setValue(cellId);
			this.outputRecord.setField(0, this.cellId);
			collector.collect(this.outputRecord);
		}
	}
}