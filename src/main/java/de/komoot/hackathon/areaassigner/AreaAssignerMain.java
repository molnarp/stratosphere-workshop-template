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

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.util.AsciiUtils;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 * 
 * @author Larysa, Moritz Kaufmann, Stephan Ewen
 */
public class AreaAssignerMain implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Converts a PactRecord containing one string in to multiple string/integer
	 * pairs. The string is tokenized by whitespaces. For each token a new
	 * record is emitted, where the token is the first field and an Integer(1)
	 * is the second field.
	 */
	@ConstantFields(fields = {})
	@OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
	public static class TokenizeLine extends MapStub {
		// initialize reusable mutable objects
		private final PactRecord outputRecord = new PactRecord();
		private final PactString word = new PactString();
		private final PactInteger one = new PactInteger(1);

		private final AsciiUtils.WhitespaceTokenizer tokenizer = new AsciiUtils.WhitespaceTokenizer();

		@Override
		public void map(PactRecord record, Collector<PactRecord> collector) {
			// get the first field (as type PactString) from the record
			PactString line = record.getField(0, PactString.class);

			// normalize the line
			AsciiUtils.replaceNonWordChars(line, ' ');
			AsciiUtils.toLowerCase(line);

			// tokenize the line
			this.tokenizer.setStringToTokenize(line);
			while (tokenizer.next(this.word)) {
				// we emit a (word, 1) pair
				this.outputRecord.setField(0, this.word);
				this.outputRecord.setField(1, this.one);
				collector.collect(this.outputRecord);
			}
		}
	}

	/**
	 * Sums up the counts for a certain given key. The counts are assumed to be
	 * at position <code>1</code> in the record. The other fields are not
	 * modified.
	 */
	@ConstantFields(fields = { 0 })
	@OutCardBounds(lowerBound = 1, upperBound = 1)
	@Combinable
	public static class CountWords extends ReduceStub {
		private final PactInteger cnt = new PactInteger();

		@Override
		public void reduce(Iterator<PactRecord> records,
				Collector<PactRecord> out) throws Exception {
			PactRecord element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				PactInteger i = element.getField(1, PactInteger.class);
				sum += i.getValue();
			}

			this.cnt.setValue(sum);
			element.setField(1, this.cnt);
			out.collect(element);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.pact.common.stubs.ReduceStub#combine(java.util.Iterator
		 * , eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void combine(Iterator<PactRecord> records,
				Collector<PactRecord> out) throws Exception {
			// the logic is the same as in the reduce function, so simply call
			// the reduce method
			this.reduce(records, out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public Plan getPlan(String... args) {
		// parse job parameters
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String nodeDataInput = (args.length > 1 ? args[1] : "");
		String areaDataInput = (args.length > 2 ? args[1] : "");
		String output = (args.length > 3 ? args[2] : "");
//  Input source
		FileDataSource nodeSource = new FileDataSource(TextInputFormat.class,
				nodeDataInput, "Input Lines");
		FileDataSource areaSource = new FileDataSource(TextInputFormat.class,
				areaDataInput, "Input Lines");

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
                MatchContract idMatcher = MatchContract.builder(IdMatcher.class, PactInteger.class, 0,0)
                        .input1(nodeCellId).input2(areaCellId).name("Matching by Cell Ids").build();
//              Reduce
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
		return "Parameters: [noSubStasks] [nodeinput] [areainput] [output]";
	}

}
