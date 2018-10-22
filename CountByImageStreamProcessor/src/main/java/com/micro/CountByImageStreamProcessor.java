
package com.micro;

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

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
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
public class CountByImageStreamProcessor {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(CountByImageStreamProcessor.class, args); 
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "count_by_imageid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.33.116.79:32777");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Gson gson= new Gson();
        Type  mapType= new TypeToken<Map<String,Object>>(){}.getType();
        Type listType= new TypeToken<List<Map<String,Object>>>(){}.getType();
        final StreamsBuilder builder = new StreamsBuilder();
        Map<String, Object> serdeProps = new HashMap<>();
        
        
        
//        final Serializer<Container> containerSer = new JsonPOJOSerializer<>();
//        serdeProps.put("JsonPOJOClass", Container.class);
//        containerSer.configure(serdeProps, false);
//        
//        final Deserializer<Container> containerDeser = new JsonPOJODeserializer<>();
//        serdeProps.put("JsonPOJOClass", Container.class);
//        containerDeser.configure(serdeProps, false);
//        
//        final Serde<Container> containerSerDe = Serdes.serdeFrom(containerSer, containerDeser); 
//        
//        final Serializer<List<Container>> containerListSer = new JsonPOJOSerializer<>();
//        serdeProps.put("JsonPOJOClass", List.class);
//        containerListSer.configure(serdeProps, false);
//        
//        final Deserializer<List<Container>> containerListDeser = new JsonPOJODeserializer<>();
//        serdeProps.put("JsonPOJOClass",List.class);
//        containerListDeser.configure(serdeProps, false);
//        
//        final Serde<List<Container>> containerListSerDe = Serdes.serdeFrom(containerListSer, containerListDeser); 
//  
//        
//        final Serializer<Map<String, Object>> conser = new JsonPOJOSerializer<>();
//        serdeProps.put("JsonPOJOClass", LinkedTreeMap.class);
//        conser.configure(serdeProps, false);
//        
//        final Deserializer<Map<String, Object>> conDeser = new JsonPOJODeserializer<>();
//        serdeProps.put("JsonPOJOClass",LinkedTreeMap.class);
//        conDeser.configure(serdeProps, false);
//        
//        final Serde<Map<String, Object>> conSerDe = Serdes.serdeFrom(conser, conDeser); 
//  
//        
        
//        KStream<String, Map<String, Object>> stream1= builder.<String, String>stream("dockerapm")
//               .flatMapValues(value -> { 
//            	   Map<String, String> map=gson.fromJson(value, mapType);
//            	   String containersString=map.get("value");
//            	   List<Map<String,Object>> containerList=gson.fromJson(containersString, listType);
//            	   return containerList;}
//                    )
//                   .selectKey((k,value)->(String)value.get("imageId")+new Date().toString());
//                
//      KTable<String, Long> stream1=
//		builder.<String, String>stream("container_to_image")
//        .flatMapValues(value -> { 
//     	   Map<String, Object> map=gson.fromJson(value, mapType);
//     	  List<Map<String,Object>> containerList=(List<Map<String,Object>>)map.get("value");
//     	  return containerList;}
//             )
//        .map((k,v)->{
//            	return KeyValue.pair((String)v.get("imageId"),new Date().toString()); 
//             })
//        .groupByKey()
//        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
//stream1.toStream().to("count_by_imageid", Produced.with(Serdes.String(), Serdes.Long()));

        
        KTable<String, Long> stream1=
        		builder.<String, String>stream("container_to_image")
                .map((k,v)->{
                    	return KeyValue.pair(v,new Date().toString()); 
                     })
                .groupByKey()
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
        stream1.toStream().to("count_by_imageid", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
