package de.komoot.hackathon.areaassigner;

import eu.stratosphere.pact.common.contract.*;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 *
 */
public class AreaAssignerMain implements PlanAssembler, PlanAssemblerDescription {
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
				nodeDataInput, "Input Nodes");
		FileDataSource areaSource = new FileDataSource(TextInputFormat.class,
				areaDataInput, "Input Areas");

		//  Node mappers

		MapContract nodeInput = MapContract.builder(GeometryInput.class)
				.input(nodeSource).name("Reading node data").build();
		MapContract nodeBBox = MapContract.builder(BoundingBox.class)
				.input(nodeInput).name("Calculating Bounding Boxes").build();
		MapContract nodeCellId = MapContract.builder(CellId.class)
				.input(nodeBBox).name("Assigning CellId").build();

		//  Area mappers

		MapContract areaInput = MapContract.builder(GeometryInput.class)
				.input(areaSource).name("Reading area data").build();
		MapContract areaBBox = MapContract.builder(BoundingBox.class)
				.input(areaInput).name("Calculating Bounding Boxes").build();
		MapContract areaCellId = MapContract.builder(CellId.class)
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

		Plan plan = new Plan(out, "AreaAssigner");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	public String getDescription() {
		return "Parameters: [noSubStasks] [nodeinput] [areainput] [output]";
	}
}
