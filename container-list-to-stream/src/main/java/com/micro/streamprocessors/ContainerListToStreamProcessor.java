
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
public class ContainerListToStreamProcessor {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(ContainerListToStreamProcessor.class, args); 
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "container-list-to-stream");
        StreamProcessor
        .build()
        .withProperties(props)
        .withProcessor(
        ()->{
    	   final StreamsBuilder builder = new StreamsBuilder();
    	   Gson gson= new Gson();
           Type  mapType= new TypeToken<Map<String,Object>>(){}.getType();
           Type listType= new TypeToken<List<Map<String,Object>>>(){}.getType();
            builder.<String, String>stream("container_list")
           .flatMapValues(value ->(List<Map<String,Object>>)gson.fromJson(value,listType))
           .mapValues(v->gson.toJson(v))
           .to("container_details", Produced.with(Serdes.String(), Serdes.String()));
           return builder;
       })
       .start();
       System.exit(0);
    }
}
