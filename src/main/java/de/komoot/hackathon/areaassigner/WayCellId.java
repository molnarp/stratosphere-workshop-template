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

import com.vividsolutions.jts.geom.LineString;
import de.komoot.hackathon.areaassigner.model.PactGeometry;
import de.komoot.hackathon.areaassigner.utils.GeometryUtils;
import de.komoot.hackathon.areaassigner.utils.Grid;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.HashSet;

import java.util.Set;

/**
 * assigns a node to a cell id
 *
 * @author christoph
 */

public class WayCellId extends MapStub {
    
    private int zoom;
    
	private final PactRecord outputRecord = new PactRecord();
	private final PactString cellId = new PactString();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        zoom = parameters.getInteger("zoom", 12);
    }

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		PactString itemId = record.getField(0, PactString.class);
		PactGeometry line = record.getField(1, PactGeometry.class);

		this.outputRecord.setField(1, itemId);
		this.outputRecord.setField(2, line);

        Set<String> cellIds = new HashSet<>();
        for (LineString part : GeometryUtils.splitLine((LineString) line.getGeometry())) {
            cellIds.addAll(Grid.getCellIds(part, zoom));
        }
        
        for(String cellId : cellIds) {
            this.cellId.setValue(cellId);
            this.outputRecord.setField(0, this.cellId);
            collector.collect(this.outputRecord);
        }
	}
}