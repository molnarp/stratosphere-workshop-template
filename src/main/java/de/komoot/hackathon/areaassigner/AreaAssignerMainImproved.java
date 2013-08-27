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
import eu.stratosphere.pact.common.type.base.PactString;

/**
 *
 */
public class AreaAssignerMainImproved implements PlanAssembler, PlanAssemblerDescription {
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

    //  Input source
    FileDataSource nodeSource = new FileDataSource(TextInputFormat.class,
        nodeDataInput, "Node FileDataSource");
    FileDataSource areaSource = new FileDataSource(TextInputFormat.class,
        areaDataInput, "Area FileDataSource");

    //  Node mappers
    MapContract nodeInput = MapContract.builder(GeometryInput.class)
        .input(nodeSource).name("GeometryInput").build();
    MapContract nodeCellId = MapContract.builder(StartingCellMap.class)
        .input(nodeInput).name("StartingCellMap").build();

    //  Area mappers
    MapContract areaInput = MapContract.builder(GeometryInput.class)
        .input(areaSource).name("GeometryInput").build();
    MapContract areaCellId = MapContract.builder(StartingCellMap.class)
        .input(areaInput).name("StartingCellMap").build();

    // Reduce - count geometry per cellids
    ReduceContract countReducer = ReduceContract.builder(CellCounter.class,
        PactString.class, 0).input(nodeCellId, areaCellId).name("CellCounter").build();

    // Refining the grid
    CoGroupContract gridCogroup = CoGroupContract.builder(GridCoGroup.class, PactString.class, 0, 0)
        .input1(countReducer)
        .input2(nodeCellId, areaCellId)
        .name("GridCoGroup")
        .build();

    // secound round
    /*
    // Reduce - count geometry per cellids
    ReduceContract countReducer2 = ReduceContract.builder(CellCounter.class,
        PactString.class, 0).input(gridCogroup).name("CellCounter 2nd round").build();

    // Refining the grid
    CoGroupContract gridCogroup2 = CoGroupContract.builder(GridCoGroup.class, PactString.class, 0, 0)
        .input1(countReducer2)
        .input2(gridCogroup)
        .name("GridCoGroup 2nd round")
        .build();
        */

    // Separate the input
    MapContract nodeSeparate = MapContract.builder(NodeSeparatorMap.class)
        .input(gridCogroup).name("NodeSeparatorMap").build();
    
    // Separate the input
    MapContract areaSeparate = MapContract.builder(AreaSeparatorMap.class)
        .input(gridCogroup).name("AreaSeparatorMap").build();

    // Id Matcher
    MatchContract idMatcher = MatchContract.builder(IdMatcher.class, PactString.class, 0, 0)
        .input1(nodeSeparate).input2(areaSeparate).name("IdMatcher").build();

    // Reduce
    ReduceContract nodeReducer = ReduceContract.builder(NodeReducer.class,
        PactString.class, 0).input(idMatcher).name("NodeReducer").build();

    // Output
    FileDataSink out = new FileDataSink(RecordOutputFormat.class, output,
        nodeReducer, "FileDataSink");

    RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
    .fieldDelimiter(',').lenient(true)
    .field(PactString.class, 0)
    .field(PactString.class, 1);

    Plan plan = new Plan(out, "AreaAssignerImproved");
    plan.setDefaultParallelism(noSubTasks);
    return plan;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return "Parameters: [noSubStasks] [nodeinput] [areainput] [output]";
  }
}
