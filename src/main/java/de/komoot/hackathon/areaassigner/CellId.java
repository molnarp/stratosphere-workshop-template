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
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.Arrays;

import java.util.List;

/**
 * assigns a node to a cell id
 *
 * @author christoph
 */

public class CellId extends MapStub {
    
        public static final int GRID_SIZE = 10;
	private final PactRecord outputRecord = new PactRecord();
	private final PactString cellId = new PactString();
	private Grid grid = new Grid(GRID_SIZE);

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		PactString itemId = record.getField(0, PactString.class);
		PactGeometry point = record.getField(1, PactGeometry.class);

		this.outputRecord.setField(1, itemId);
		this.outputRecord.setField(2, point);

		List<String> cellIds = grid.getIdsForGeometry(point.getGeometry());
		for(String cellId : cellIds) {
			this.cellId.setValue(cellId);
			this.outputRecord.setField(0, this.cellId);
			collector.collect(this.outputRecord);
		}
	}
}