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

package org.apache.flink.streaming.examples.autoscale;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/**
 * Example.
 */
public class ScaleDownExample {
	public static void main(String[] args) throws Exception {
		// get the execution environment
		Configuration config = new Configuration();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

		env.enableCheckpointing(10000);
		DataStream<String> source = env.fromCollection(new ScaleUpExample.MyIterator(), String.class);
		source = source.union(source, source);
		source.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				Thread.sleep(100);
				return value;
			}
		}).setParallelism(100).setMaxParallelism(128)
				.addSink(new DiscardingSink<>()).setParallelism(100);
		source.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				Thread.sleep(100);
				return value;
			}
		}).setParallelism(50).setMaxParallelism(128)
				.addSink(new DiscardingSink<>()).setParallelism(50);

		env.execute();
	}

}
