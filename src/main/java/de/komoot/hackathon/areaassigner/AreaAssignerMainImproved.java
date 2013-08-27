package de.komoot.hackathon.areaassigner;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
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
public class AreaAssignerMainImproved implements PlanAssembler, PlanAssemblerDescription {
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
    MapContract nodeCellId = MapContract.builder(StartingCellMap.class)
        .input(nodeInput).name("Calculating starting cell ids.").build();

    //  Area mappers
    MapContract areaInput = MapContract.builder(GeometryInput.class)
        .input(areaSource).name("Reading area data").build();
    MapContract areaCellId = MapContract.builder(StartingCellMap.class)
        .input(areaInput).name("Calculating starting cell ids.").build();


/*

    // Reduce - count geometry per cellids
    ReduceContract countReducer = ReduceContract.builder(CellCounter.class,
        PactString.class, 0).input(nodeCellId, areaCellId).name("Counting the geomerty to cellids.").build();

    // Refining the grid
    CoGroupContract gridCogroup = CoGroupContract.builder(GridCoGroup.class, PactString.class, 0, 0)
        .input1(countReducer)
        .input2(nodeCellId, areaCellId)
        .name("Refining the grid based on density.")
        .build();

    // secound round
    // Reduce - count geometry per cellids
    ReduceContract countReducer2 = ReduceContract.builder(CellCounter.class,
        PactString.class, 0).input(gridCogroup).name("Counting the geomerty to cellids.").build();

    // Refining the grid
    CoGroupContract gridCogroup2 = CoGroupContract.builder(GridCoGroup.class, PactString.class, 0, 0)
        .input1(countReducer2)
        .input2(gridCogroup)
        .name("Refining the grid based on density.")
        .build();


    // Separate the input
    MapContract nodeSeparate = MapContract.builder(NodeSeparatorMap.class)
        .input(gridCogroup2).name("Separate the node from geometry.").build();
    //nodeSeparate.getCompilerHints().setUniqueField(new FieldSet(0));
    // Separate the input
    MapContract areaSeparate = MapContract.builder(AreaSeparatorMap.class)
        .input(gridCogroup2).name("Separate the area from geometry.").build();
*/
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
    .fieldDelimiter(',').lenient(true)
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
