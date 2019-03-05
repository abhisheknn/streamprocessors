/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.micro.streamprocessors;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import com.micro.kafka.StreamProcessor;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class ContainerIdToImageStreamProcessor {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(ContainerIdToImageStreamProcessor.class, args);
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "containerId_to_imageId");
		StreamProcessor.build().withProperties(props).withProcessor(()->{
			Gson gson = new Gson();
			Type mapType = new TypeToken<Map<String, Object>>() {
			}.getType();
			Type listType = new TypeToken<List<Map<String, Object>>>() {
			}.getType();
			final StreamsBuilder builder = new StreamsBuilder();	
			builder.<String, String>stream("container_details").map((k, v) -> {
				Map container = gson.fromJson(v, mapType);
				return KeyValue.pair(k+"_"+container.get("id"), (String) container.get("image"));     // Need to test the impact of this .
			}).to("containerid_to_imageid", Produced.with(Serdes.String(), Serdes.String()));

			return builder;
		}).start();
}
}
