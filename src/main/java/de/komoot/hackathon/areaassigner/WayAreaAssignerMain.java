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

import eu.stratosphere.pact.common.contract.*;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactString;
import org.apache.log4j.Logger;

/**
 *
 */
public class WayAreaAssignerMain implements PlanAssembler, PlanAssemblerDescription {
	/**
	 * {@inheritDoc}
	 */
    @Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String nodeDataInput = (args.length > 1 ? args[1] : "");
		String areaDataInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");
		int zoom = (args.length > 4 ? Integer.parseInt(args[4]) : 12);

		//  Input source
		
		FileDataSource nodeSource = new FileDataSource(TextInputFormat.class,
				nodeDataInput, "Input Ways");
		FileDataSource areaSource = new FileDataSource(TextInputFormat.class,
				areaDataInput, "Input Areas");

		//  Way mappers

		MapContract wayInput = MapContract.builder(GeometryInput.class)
				.input(nodeSource).name("Reading way data").build();
		MapContract wayCellId = MapContract.builder(WayCellId.class)
				.input(wayInput).name("Assigning CellId").build();
        wayCellId.setParameter("zoom", zoom);

		//  Area mappers

		MapContract areaInput = MapContract.builder(GeometryInput.class)
				.input(areaSource).name("Reading area data").build();
		MapContract areaCellId = MapContract.builder(CellId.class)
				.input(areaInput).name("Assigning CellId").build();

		// Id Matcher
		MatchContract idMatcher = MatchContract.builder(IdMatcher.class, PactString.class, 0, 0)
				.input1(wayCellId).input2(areaCellId).name("Matching by Cell Ids").build();

		// Reduce
		ReduceContract wayReducer = ReduceContract.builder(NodeReducer.class,
				PactString.class, 0).input(idMatcher).name("Reduce by Way Ids").build();
		// Output
		FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,
				wayReducer, "Reduced Values");

		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
				.fieldDelimiter(',').lenient(true)
				.field(PactString.class, 0)
				.field(PactString.class, 1);

		Plan plan = new Plan(out, "WayAreaAssigner");
		plan.setDefaultParallelism(noSubTasks);
        
        final Logger log = Logger.getLogger("eu.stratosphere.nephele.jobmanager.JobManager");
        log.error("---- STARTING UP -----");
        
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                log.error("----- SHUTTING DOWN ------");
            }
            
        });
		return plan;
        
	}

	/**
	 * {@inheritDoc}
	 */
	public String getDescription() {
		return "Parameters: [noSubStasks] [nodeinput] [areainput] [output] [zoom]";
	}
}
