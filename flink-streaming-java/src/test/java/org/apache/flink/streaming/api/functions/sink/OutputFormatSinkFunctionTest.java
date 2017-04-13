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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.junit.Test;
import org.mockito.Mockito;

public class OutputFormatSinkFunctionTest {

	@Test
	public void setRuntimeContext() throws Exception {

		// Make sure setRuntimeContext of the rich output format is called
		RichOutputFormat<Object> mockRichOutputFormat = Mockito.mock(RichOutputFormat.class);
		OutputFormatSinkFunction<Object> objectOutputFormatSinkFunction = new OutputFormatSinkFunction<>(mockRichOutputFormat);
		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		objectOutputFormatSinkFunction.setRuntimeContext(mockContext);
		Mockito.verify(mockRichOutputFormat, Mockito.times(1)).setRuntimeContext(Mockito.eq(mockContext));

		// Make sure setRuntimeContext work well when output format is not RichOutputFormat
		OutputFormat<Object> mockNonRichOutputFormat = Mockito.mock(OutputFormat.class);
		objectOutputFormatSinkFunction = new OutputFormatSinkFunction<>(mockNonRichOutputFormat);
		objectOutputFormatSinkFunction.setRuntimeContext(mockContext);

	}

}
