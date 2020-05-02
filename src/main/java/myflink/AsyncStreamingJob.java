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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AsyncStreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment



		final StreamExecutionEnvironment env =
			StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// ...

		env.socketTextStream("localhost", 10002)
			.map(new MapFunction<String, Tuple2<Long, String>>() {
				@Override
				public Tuple2<Long, String> map(String s) throws Exception {

					String[] parts = s.split(",");

					return Tuple2.of(Long.valueOf(parts[0]), parts[1]);
				}
			})
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
				@Override
				public long extractAscendingTimestamp(Tuple2<Long, String> element) {
					return element.f0;
				}
			})
			.setParallelism(1)
			.keyBy(0)
			.window(TumblingEventTimeWindows.of(Time.seconds(2)))
//			.max(0)
//			.allowedLateness(Time.seconds(1))
			//.sum(0)
			.process(new ProcessWindowFunction<Tuple2<Long, String>, Integer, Tuple, TimeWindow>() {

				@Override
				public void process(Tuple tuple, Context context, Iterable<Tuple2<Long, String>> elements, Collector<Integer> out) throws Exception {

					int size = 0;
					int cnt = 0;
					for (Tuple2<Long, String> i : elements) {
						size += i.f1.length();
						cnt += 1;
					}

					System.out.println("~~~~elements cnt: " + cnt);

					out.collect(size);
				}
			})
			.print().setParallelism(1);

		 env.execute("Flink Streaming App");
	}
}
