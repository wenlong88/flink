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
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Example.
 */
public class ScaleUpExample {

	public static void main(String[] args) throws Exception {
		// get the execution environment
		Configuration config = new Configuration();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

		DataStream<String> source = env.fromCollection(new MyIterator(), String.class);
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

	/**
	 * Test source.
	 */
	public static class MyIterator implements Iterator<String>, Serializable {
		private int idx = 0;

		public int getIdx() {
			return idx;
		}

		public void setIdx(int idx) {
			this.idx = idx;
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public String next() {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return WordCountData.WORDS[idx++ % WordCountData.WORDS.length];
		}
	}
}
