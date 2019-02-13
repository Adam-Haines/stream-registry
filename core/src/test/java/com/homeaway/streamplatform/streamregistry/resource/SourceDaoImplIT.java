/* Copyright (c) 2018 Expedia Group.
 * All rights reserved.  http://www.homeaway.com

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *      http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.homeaway.streamplatform.streamregistry.resource;

import static com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl.SOURCE_ENTITY_EVENT_DIR;
import static com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl.SOURCE_ENTITY_PROCESSOR_APP_ID;
import static com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl.SOURCE_ENTITY_TOPIC_NAME;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import com.homeaway.streamplatform.streamregistry.utils.IntegrationTestUtils;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Preconditions;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.model.SourceType;

@Slf4j
public class SourceDaoImplIT extends BaseResourceIT {


    public static Properties commonConfig;

    public static Properties sourceProcessorConfig;

    private static SourceDaoImpl sourceDao;


    @BeforeClass
    public static void setUp() throws Exception {

        // Make sure all temp dirs are cleaned first
        // This will solve a lot of the dir locked issue etc.
        FileUtils.deleteDirectory(SourceDaoImpl.SOURCE_COMMAND_EVENT_DIR);
        FileUtils.deleteDirectory(SOURCE_ENTITY_EVENT_DIR);

        commonConfig = new Properties();
        commonConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        commonConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        commonConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        commonConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());


        sourceProcessorConfig = new Properties();
        sourceProcessorConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        sourceProcessorConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        sourceProcessorConfig.put(ConsumerConfig.GROUP_ID_CONFIG, SOURCE_ENTITY_PROCESSOR_APP_ID + "_consumer");
        sourceProcessorConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        sourceProcessorConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        sourceProcessorConfig.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        sourceProcessorConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        sourceProcessorConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        sourceProcessorConfig.put(StreamsConfig.STATE_DIR_CONFIG, SOURCE_ENTITY_EVENT_DIR.getPath());
        sourceProcessorConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);

        CompletableFuture<Boolean> initialized = new CompletableFuture<>();
        sourceDao = new SourceDaoImpl(commonConfig, () -> initialized.complete(true));
        sourceDao.start();

        log.info(
                "Waiting for processor's init method to be called (KV store created) before servicing the HTTP requests.");



        long timeoutTimestamp = System.currentTimeMillis() + TEST_STARTUP_TIMEOUT_MS;
        while (!initialized.isDone() && System.currentTimeMillis() <= timeoutTimestamp) {
            Thread.sleep(100); // wait some cycles before checking again
        }
        Preconditions.checkState(initialized.isDone(), "Did not receive state store initialized signal, aborting.");
        Preconditions.checkState(sourceDao.getSourceEntityProcessor().state().isRunning(), "State store did not start. Aborting.");
        log.info("Processor wait completed.");

    }

    @Test
    public void testSourceDaoImpl() throws Exception {

        String sourceName = "source-a";
        String streamName = "stream-a";

        Assert.assertNotNull(commonConfig);

        Source source = buildSource(sourceName, streamName, null);

        // inserting
        sourceDao.insert(source);
        // updating
        sourceDao.update(source);


        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "state-store-dsl-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        List<KeyValue<String, com.homeaway.digitalplatform.streamregistry.Source>> list = IntegrationTestUtils.readKeyValues(SOURCE_ENTITY_TOPIC_NAME, consumerConfig);
        Assert.assertThat(list.size(), greaterThan(0));


//        // starting
//        Thread.sleep(1000L);
        sourceDao.start(sourceName);
        Assert.assertThat(list.size(), greaterThan(2));

//        // pausing
//        Thread.sleep(1000L);
        sourceDao.pause(sourceName);
        Assert.assertThat(list.size(), greaterThan(3));

//        // resuming
//        Thread.sleep(1000L);
//        sourceDao.resume(sourceName);
//        // stopping
//        Thread.sleep(1000L);
//        sourceDao.stop(sourceName);






    }

    private Source buildSource(String sourceName, String streamName, String status) {

        Map<String, String> map = new HashMap<>();
        map.put("kinesis.url", "url");

        return Source.builder()
                .sourceName(sourceName)
                .streamName(streamName)
                .sourceType(SourceType.SOURCE_TYPES.get(0))
                .status(status)
                .imperativeConfiguration(map)
                .tags(map)
                .build();
    }
}
