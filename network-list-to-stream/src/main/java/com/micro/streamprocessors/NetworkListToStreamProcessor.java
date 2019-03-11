
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
import org.springframework.util.SystemPropertyUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import com.micro.cassandra.Cassandra;
import com.micro.cassandra.Cassandra.CONFIGURATION_TYPE;
import com.micro.cassandra.Cassandra.Configuration;
import com.micro.kafka.KafkaProducer;
import com.micro.kafka.StreamProcessor;

import java.lang.reflect.Type;
import java.util.ArrayList;
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
public class NetworkListToStreamProcessor {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(NetworkListToStreamProcessor.class, args); 
    	createTable();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "network-list-to-stream3");
        StreamProcessor
        .build()
        .withProperties(props)
        .withProcessor(()->{
        	Gson gson= new GsonBuilder().disableHtmlEscaping().create();;
            Type  mapType= new TypeToken<Map<String,Object>>(){}.getType();
            Type listType= new TypeToken<List<Map<String,Object>>>(){}.getType();
            final StreamsBuilder builder = new StreamsBuilder();
            Map<String, Object> serdeProps = new HashMap<>();
            builder.<String, String>stream("dockerx.network_list")
            .flatMapValues(value ->(List<Map<String,Object>>)gson.fromJson(value,listType))
            .map((k,v)->{ v.put("macaddress", k) ; return KeyValue.pair(k, gson.toJson(v));})
             .to("dockerx.network_details", Produced.with(Serdes.String(), Serdes.String()));
            return builder;
        })
        .start();
  }
    
	private static void createTable() {
		
//		String obj = "{\"id\":\"string\",\"labels\":{},\"name\":\"string\",\"scope\":\"string\""
//				+ ",\"driver\":\"string\",\"enableIPv6\":\"boolean\",\"internal\":\"boolean\","
//				+ "\"ipam\":{\"driver\":\"string\",\"config\":[{\"subnet\":\"string\",\"gateway\":\"string\"}]},"
//				+ "\"containers\":{\"string\":{\"endpointId\":\"string\",\"macAddress\":\"string\",\"ipv4Address\":\"string\",\"ipv6Address\":\"\"}},"
//				+ "\"options\":{\"com.docker.network.bridge.default_bridge\":\"true\","
//				+ "\"com.docker.network.bridge.enable_icc\":\"true\","
//				+ "\"com.docker.network.bridge.enable_ip_masquerade\":\"true\","
//				+ "\"com.docker.network.bridge.host_binding_ipv4\":\"0.0.0.0\","
//				+ "\"com.docker.network.bridge.name\":\"docker0\",\"com.docker.network.driver.mtu\":\"1500\"},"
//				+ "\"attachable\":false}";
//		
		Map<String, String> columns = new HashMap<>();
		
		columns.put("id" ,"text");
		columns.put("labels" ,"map<text,text>");
		columns.put("name" ,"text");
		columns.put("scope" ,"text");
		columns.put("driver" ,"text");
		columns.put("enableIPv6" ,"boolean");
		columns.put("internal" ,"boolean");
		columns.put("ipam" ,"frozen<network_ipam_info>");
		columns.put("containers" ,"map<text,frozen<network_container_info>>");
		columns.put("options" ,"map<text,text>");
		columns.put("attachable" ,"boolean");
		columns.put("macaddress" ,"text");
		columns.put("PRIMARY KEY" ,"(macaddress, id)");
		
		Properties producerConfig = new Properties();
		producerConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKABROKERS"));
		
		Cassandra.Configuration conf = new Configuration();
		conf.setConf(columns);
		conf.setName("network_details");
		conf.setKeySpace("dockerx");
		Map<String, Object> tableConf= new HashMap<>();
		tableConf.put(Cassandra.CONFIGURATION_TYPE.TABLE.toString(),conf);
		createContainerNetworkConfigType(tableConf);
		createIPAMType(tableConf);
		KafkaProducer.build().withConfig(producerConfig).produce("create-table", "network_details", tableConf);
	}

	private static void createContainerNetworkConfigType(Map<String, Object> tableConf) {
		List<Configuration> typeConf= new ArrayList<>();
		Map<String, String> typeConfiguration= new HashMap<>();
		typeConfiguration.put("endpointId", "text");
		typeConfiguration.put("macAddress", "text");
		typeConfiguration.put("ipv4Address", "text");
		typeConfiguration.put("ipv6Address", "text");
		Cassandra.Configuration conf = new Configuration();
		conf.setConf(typeConfiguration);
		conf.setName("network_container_info");
		conf.setKeySpace("dockerx");
		typeConf.add(conf);
		tableConf.put(CONFIGURATION_TYPE.TYPE.toString(), typeConf);
	}
	
	private static void createIPAMType(Map<String, Object> tableConf) {
		Map<String, String> typeConfiguration= new HashMap<>();
		typeConfiguration.put("driver", "text");
		typeConfiguration.put("config", "frozen<list<frozen<map<text,text>>>>");
		typeConfiguration.put("options ", "map<text,text>");
		Cassandra.Configuration conf = new Configuration();
		conf.setConf(typeConfiguration);
		conf.setName("network_ipam_info");
		conf.setKeySpace("dockerx");
		((List)tableConf.get(CONFIGURATION_TYPE.TYPE.toString())).add(conf);
	}
	
}
