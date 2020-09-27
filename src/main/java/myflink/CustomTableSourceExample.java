/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myflink;

import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.calcite.shaded.com.google.common.primitives.Ints;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class CustomTableSourceExample {

	public static void main(String[] args) throws Exception {
//		EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
//		TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
//
//
//		bbTableEnv.registerTableSource("t1", new LuceneTableSource());
//
//		bbTableEnv.sqlQuery("select * from t1").printSchema();

      ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
      // fbEnv.setParallelism(30);
	  BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

	  // TODO: execute SQL ?
      fbTableEnv.registerTableSource("t1", new LuceneTableSource());

      fbTableEnv.toDataSet(fbTableEnv.sqlQuery("select name, age, info from t1"), Row.class).print();
	}

	// https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sourceSinks.html#defining-a-batchtablesource
	// https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/sourceSinks.html#defining-a-tablesource-with-projection-push-down


	public static class LuceneTableSource extends InputFormatTableSource implements ProjectableTableSource {


		private int[] selectedFields = {};

		public LuceneTableSource() {}

		public LuceneTableSource(int[] selectedFields) {
			this.selectedFields = selectedFields;
		}


		@Override
		public DataType getProducedDataType() {
			return getTableSchema().toRowDataType();
		}

		@Override
		public InputFormat getInputFormat() {
			return new LuceneInputFormat2(this.selectedFields);
		}

		@Override
		public TableSchema getTableSchema() {

			String[] names = {"info", "name", "age"};

			DataType[] dataTypes = {DataTypes.STRING(), DataTypes.STRING(), DataTypes.BIGINT()};
			TableSchema tableSchema = TableSchema.builder().fields(names, dataTypes).build();
			return tableSchema;
		}

		// https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sourceSinks.html#defining-a-tablesource-with-projection-push-down
		@Override
		public TableSource projectFields(int[] ints) {

			if (ints.length == 0) {
				return new LuceneTableSource();
			}

			return new LuceneTableSource(ints);
		}
	}

	// TODO:
	// 	FileInputFormat, BinaryInputFormat,
	//  Lucene 是一个Folder, 如何提高并行度。
	//  ES Logical Index Dir
	//      ES Phy Index Dir
	//          Shard/Lucene Dir
	//              Segment File(binary)
	public static class LuceneInputFormat extends FileInputFormat<Row> {

		@Override
		public boolean reachedEnd() throws IOException {
			return false;
		}

		@Override
		public Row nextRecord(Row reuse) throws IOException {
			return null;
		}

//		public void test() {
//			setFilePath();
//		}
	}


	public static class LuceneInputFormat3 extends BinaryInputFormat<Row> {

		@Override
		protected Row deserialize(Row reuse, DataInputView dataInput) throws IOException {
			return null;
		}
	}


	public static class LuceneInputFormat2 extends GenericInputFormat<Row> {

		private int count = 0;

		private int[] selectedFields = {};

		public LuceneInputFormat2() {}

		public LuceneInputFormat2(int[] selectedFields) {
			this.selectedFields = selectedFields;
		}

		@Override
		public void open(GenericInputSplit split) throws IOException {
			super.open(split);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return count >= 3;
		}

		@Override
		public Row nextRecord(Row s) throws IOException {

			// TODO: 不确定，包含ProjectionPushdown的是不是这么实现？

			count += 1;

			Object[] data = new Object[3];

			if (selectedFields.length > 0) {
				for (int i = 0; i < 3; i ++) {

					if (Ints.asList(selectedFields).contains(i)) {
						switch (i) {
							case 0:
								data[i] = "2020";
								break;
							case 1:
								data[i] = "gary";
								break;
							case 2:
								data[i] = 33L;
								break;
						}
					} else {
						data[i] = null;
					}
				}
			} else {
				return Row.of("2020", "gary", 33L);
			}


			return Row.of(data);
		}

		@Override
		public GenericInputSplit[] createInputSplits(int numSplits) {
			// TODO: 如何划分lucene split，如何获取 lucene hdfs path, mappings等额外信息。
			GenericInputSplit[] splits = new GenericInputSplit[1];
			splits[0] = new GenericInputSplit(0, 1);
			return splits;
		}
	}
}
