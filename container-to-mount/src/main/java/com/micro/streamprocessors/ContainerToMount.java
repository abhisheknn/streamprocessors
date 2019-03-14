
package com.micro.streamprocessors;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.micro.cassandra.Cassandra;
import com.micro.cassandra.Cassandra.Configuration;
import com.micro.kafka.KafkaProducer;
import com.micro.kafka.StreamProcessor;

@SpringBootApplication
public class ContainerToMount{

    private static final String CONTAINERID = "CONTAINERID";

	public static void main(String[] args) throws Exception {
        SpringApplication.run(ContainerToMount.class, args); 
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "container_to_mount");
        createTable();
        StreamProcessor
        .build()
        .withProperties(props)
        .withProcessor(()->{
            Gson gson= new Gson();
            Type  mapType= new TypeToken<Map<String,Object>>(){}.getType();
            Type listType= new TypeToken<List<Map<String,Object>>>(){}.getType();
            final StreamsBuilder builder = new StreamsBuilder();
            builder.<String,String>stream("dockerx.container_details")
            .mapValues(v->(Map)gson.fromJson(v, mapType))
    	    .filter((k,v)-> null!=v.get("mounts") || ((List)v.get("mounts")).isEmpty())
    	    .map((k, v) -> KeyValue.pair(k+"_"+v.get("id"),gson.toJson(v.get("mounts"))))    
            .flatMapValues(v->(List<Map<String, Object>>) gson.fromJson(v, listType))
    	    .map((k, v) -> {
            	String[] ids= k.split("_");
    	    	v.put("macaddress",ids[0]);
    	    	v.put("container_id",ids[1]);
    	    	v.put("uid", UUID.randomUUID());
    	    	return KeyValue.pair(ids[0],gson.toJson(v));
    	    	})
    	    	.to("dockerx.container_to_mount", Produced.with(Serdes.String(), Serdes.String()));
            return builder;
        	})
        .start();
	
	}
	
	
	private static void createTable() {
		Gson gson = new Gson();
		Type mapType = new TypeToken<Map<String, Object>>() {
		}.getType();
		Type listType = new TypeToken<List<Map<String, Object>>>() {
		}.getType();
		String obj = "{\"name\":\"string\",\"driver\":\"string\",\"source\":\"string\",\"destination\":\"string\",\"mode\":\"string\",\"rw\":boolean,\"propagation\":\"string\",\"macaddress\":\"string\",\"container_id\":\"string\"}";
		Map<String, Object> container = gson.fromJson(obj, mapType);
		Map<String, String> columns = new HashMap<>();
		Set<String> keys = container.keySet();
		for (String key : keys) {
			columns.put(key, "text");
			if (key.equals("rw")) {
				columns.put(key, "boolean");
			}
		}
		columns.put("macaddress" ,"text");
		columns.put("uid" ,"UUID");
		columns.put("PRIMARY KEY" ,"(macaddress, container_id,uid)");
		Properties producerConfig = new Properties();
		producerConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKABROKERS"));
		
		Cassandra.Configuration conf = new Configuration();
		conf.setConf(columns);
		conf.setName("container_to_mount");
		conf.setKeySpace("dockerx");
		Map<String, Object> tableConf= new HashMap<>();
		tableConf.put(Cassandra.CONFIGURATION_TYPE.TABLE.toString(),conf);
		KafkaProducer.build().withConfig(producerConfig).produce("create-table", "container_to_mount", tableConf);
	}
	
}
