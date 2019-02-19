
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
public class ContainerToMount{

    private static final String CONTAINERID = "CONTAINERID";

	public static void main(String[] args) throws Exception {
        SpringApplication.run(ContainerToMount.class, args); 
    	Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "container_to_mount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKABROKERS"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Gson gson= new Gson();
        Type  mapType= new TypeToken<Map<String,Object>>(){}.getType();
        Type listType= new TypeToken<List<Map<String,Object>>>(){}.getType();
        final StreamsBuilder builder = new StreamsBuilder();
        Map<String, Object> serdeProps = new HashMap<>();
		
//	    builder.<String, String>stream("container_details")
//	    .flatMap((k,v)->{
//	    	Map container = gson.fromJson(v, mapType);
//	    		return container;
//	    	})
//	    .filter((k,v)->{
//	    	if(null==container.get("mounts")) return false;
//	    	return true;})
//	    .map((k, v) -> {
//			Map container = gson.fromJson(v, mapType);
//			return KeyValue.pair(k,(String)container.get("mounts"));    
//		}).to("container_to_mount", Produced.with(Serdes.String(), Serdes.String()));
//	      
//	    
        builder.<String,String>stream("container_details")
	    .mapValues(v->(Map)gson.fromJson(v, mapType))
	    .filter((k,v)-> null==v.get("mounts"))
	    .map((k, v) -> KeyValue.pair(k+"_"+v.get("id"),gson.toJson(v.get("mounts"))))    
	    .to("container_to_mount", Produced.with(Serdes.String(), Serdes.String()));

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
