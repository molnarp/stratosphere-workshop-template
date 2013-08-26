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
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 *
 */
public class AreaAssignerMain implements PlanAssembler, PlanAssemblerDescription {
	/**
	 * {@inheritDoc}
	 */
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String nodeDataInput = (args.length > 1 ? args[1] : "");
		String areaDataInput = (args.length > 2 ? args[2] : "");
		String output = (args.length > 3 ? args[3] : "");
		
		//  Input source
		FileDataSource nodeSource = new FileDataSource(TextInputFormat.class,
				nodeDataInput, "Input Nodes");
		FileDataSource areaSource = new FileDataSource(TextInputFormat.class,
				areaDataInput, "Input Areas");

		//  Node mappers

		MapContract nodeInput = MapContract.builder(GeometryInput.class)
				.input(nodeSource).name("Reading node data").build();
		MapContract nodeBBox = MapContract.builder(BoundingBox.class)
				.input(nodeInput).name("Calculating Bounding Boxes").build();
		MapContract nodeCellId = MapContract.builder(NodeCellId.class)
				.input(nodeBBox).name("Assigning CellId").build();
		
		//  Area mappers

		MapContract areaInput = MapContract.builder(GeometryInput.class)
				.input(areaSource).name("Reading area data").build();
		MapContract areaBBox = MapContract.builder(BoundingBox.class)
				.input(areaInput).name("Calculating Bounding Boxes").build();
		MapContract areaCellId = MapContract.builder(AreaCellId.class)
				.input(areaBBox).name("Assigning CellId").build();
		
		// Id Matcher
		MatchContract idMatcher = MatchContract.builder(IdMatcher.class, PactString.class, 0, 0)
				.input1(nodeCellId).input2(areaCellId).name("Matching by Cell Ids").build();
		
		// Reduce
		ReduceContract nodeReducer = ReduceContract.builder(NodeReducer.class,
				PactString.class, 0).input(idMatcher).name("Reduce by Node Ids").build();
		// Output
		FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,
				nodeReducer, "Reduced Values");

		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
				.fieldDelimiter('#').lenient(true)
				.field(PactString.class, 0)
				.field(PactString.class, 1);

		Plan plan = new Plan(out, "WordCount Example");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	public String getDescription() {
		return "Parameters: [noSubStasks] [input] [output]";
	}
}
