
package com.micro.streamprocessors;

import org.apache.kafka.common.config.ConfigException;
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
import org.springframework.util.SystemPropertyUtils;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import com.micro.cassandra.Cassandra;
import com.micro.cassandra.Cassandra.CONFIGURATION_TYPE;
import com.micro.cassandra.Cassandra.Configuration;
import com.micro.kafka.KafkaProducer;
import com.micro.kafka.StreamProcessor;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class ContainerListToStreamProcessor {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(ContainerListToStreamProcessor.class, args);
		createTable();
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "container-list-to-stream");
		StreamProcessor.build().withProperties(props).withProcessor(() -> {
			final StreamsBuilder builder = new StreamsBuilder();
			Gson gson = new Gson();
			Type mapType = new TypeToken<Map<String, Object>>() {
			}.getType();
			Type listType = new TypeToken<List<Map<String, Object>>>() {
			}.getType();
			builder.<String, String>stream("container_list")
					.flatMapValues(value -> (List<Map<String, Object>>) gson.fromJson(value, listType))
					.mapValues(v -> gson.toJson(v))
					.to("container_details", Produced.with(Serdes.String(), Serdes.String()));
			return builder;
		}).start();

		
		
	}

	private static void createMountInfoType(Map<String, Object> tableConf) {
		//String mountInfoType="CREATE TYPE dockerx.mount_info (source text,destination text,mode text,rw boolean,propagation text)";
		Map<String, String> typeConfiguration= new HashMap<>();
		typeConfiguration.put("source", "text");
		typeConfiguration.put("destination", "text");
		typeConfiguration.put("mode", "text");
		typeConfiguration.put("rw", "boolean");
		typeConfiguration.put("propagation", "text");
		Cassandra.Configuration conf = new Configuration();
		conf.setConf(typeConfiguration);
		conf.setName("container_mount_info");
		conf.setKeySpace("dockerx");
		tableConf.put(CONFIGURATION_TYPE.TYPE.toString(), conf);
	}

	private static void createTable() {
		Gson gson = new Gson();
		Type mapType = new TypeToken<Map<String, Object>>() {
		}.getType();
		Type listType = new TypeToken<List<Map<String, Object>>>() {
		}.getType();
		String obj = "{\"command\":\"string\",\"created\":1540482286,\"id\":\"string\",\"image\":\"string\",\"imageId\":\"string\",\"names\":[\"string\"],\"ports\":[{\"ip\":\"string\",\"privatePort\":\"string\",\"publicPort\":\"string\",\"type\":\"string\"}],\"labels\":{\"key\":\"value\"},\"status\":\"Up Less than a second\",\"state\":\"running\",\"hostConfig\":{\"networkMode\":\"string\"},\"networkSettings\":{\"networks\":{\"bridge\":{\"networkID\":\"string\",\"endpointId\":\"string\",\"gateway\":\"string\",\"ipAddress\":\"string\",\"ipPrefixLen\":16,\"ipV6Gateway\":\"\",\"globalIPv6Address\":\"\",\"globalIPv6PrefixLen\":0,\"macAddress\":\"string\"}}},\"mounts\":[{\"source\":\"string\",\"destination\":\"string\",\"mode\":\"\",\"rw\":true,\"propagation\":\"\",\"name\":\"string\",\"driver\":\"local\"},{\"source\":\"string\",\"destination\":\"string\",\"mode\":\"\",\"rw\":true,\"propagation\":\"string\"},{\"source\":\"string\",\"destination\":\"string\",\"mode\":\"\",\"rw\":true,\"propagation\":\"string\"}]}";
		Map<String, Object> container = gson.fromJson(obj, mapType);
		Map<String, String> columns = new HashMap<>();
		Set<String> keys = container.keySet();
		for (String key : keys) {
			columns.put(key, "text");
			if (key.equals("created")) {
				columns.put(key, "bigint");
			}
			if (key.equals("names")) {
				columns.put(key, "list<text>");
			}
			if (key.equals("ports")) {
				columns.put(key, "list<frozen<map<text,text>>>");
			}
			if (key.equals("labels")) {
				columns.put(key, "map<text,text>");
			}
			if (key.equals("hostConfig")) {
				columns.put(key, "map<text,text>");
			}
			if (key.equals("networkSettings")) {
				columns.put(key, "map<text,frozen<map<text,frozen<map<text,text>>>>>");
			}
			if (key.equals("mounts")) {
				columns.put(key, "list<frozen<container_mount_info>");
			}
		}
		columns.put("macaddress" ,"text");
		columns.put("PRIMARY KEY" ,"(macaddress, id)");
		Properties producerConfig = new Properties();
		producerConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKABROKERS"));
		
		Cassandra.Configuration conf = new Configuration();
		conf.setConf(columns);
		conf.setName("container_details");
		conf.setKeySpace("dockerx");
		Map<String, Object> tableConf= new HashMap<>();
		tableConf.put(Cassandra.CONFIGURATION_TYPE.TABLE.toString(),conf);
		createMountInfoType(tableConf);
		KafkaProducer.build().withConfig(producerConfig).produce("create-table", "container_details", tableConf);
	}
}
