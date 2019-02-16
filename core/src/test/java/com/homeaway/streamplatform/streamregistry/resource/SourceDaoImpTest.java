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

import static com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl.SOURCE_ENTITY_PROCESSOR_APP_ID;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Before;
import org.junit.Test;

import com.homeaway.digitalplatform.streamregistry.Header;
import com.homeaway.digitalplatform.streamregistry.Source;
import com.homeaway.digitalplatform.streamregistry.SourceCreateRequested;
import com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl;

@Slf4j
public class SourceDaoImpTest {

    private static SourceDaoImpl sourceDao;

    private TopologyTestDriver topologyTestDriver;
    private SchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
    private SpecificAvroSerde<SpecificRecord> specificAvroSerde;
    private Serde<Source> sourceEntitySerde;
    private static final File KSTREAMS_PROCESSOR_DIR = new File("/tmp/source-processor");


    @Before
    public void setUp() throws Exception {

        FileUtils.deleteDirectory(KSTREAMS_PROCESSOR_DIR);

        Properties commonConfig = new Properties();
        commonConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        commonConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy:8080");
        commonConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, SOURCE_ENTITY_PROCESSOR_APP_ID);
        commonConfig.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        commonConfig.put(StreamsConfig.STATE_DIR_CONFIG, KSTREAMS_PROCESSOR_DIR.getPath());
        commonConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        commonConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        Map<String, String> configMap = new HashMap<>();

        commonConfig.forEach((k, v) -> configMap.put(k.toString(), v.toString()));

        specificAvroSerde = new SpecificAvroSerde<>(mockSchemaRegistryClient);
        specificAvroSerde.configure(configMap, false);

        commonConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        commonConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, specificAvroSerde.serializer());
        commonConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        commonConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, specificAvroSerde.deserializer());

        sourceDao = new SourceDaoImpl(commonConfig, null);

        sourceEntitySerde = new SpecificAvroSerde<>(mockSchemaRegistryClient);
        sourceEntitySerde.configure(configMap, false);

        StreamsBuilder builder = new StreamsBuilder();
        sourceDao.declareStreamProcessor(builder, specificAvroSerde, sourceEntitySerde);

        topologyTestDriver = new TopologyTestDriver(builder.build(), commonConfig);
    }

    @Test
    public void testTopology() {


        final String sourceName = "source-a";
        final String streamName = "stream-a";

        ConsumerRecordFactory<String, SpecificRecord> sourceCreateConsumerFactory =
                new ConsumerRecordFactory<>(SourceDaoImpl.SOURCE_COMMANDS_TOPIC_NAME,
                        new StringSerializer(), specificAvroSerde.serializer());

        SourceCreateRequested sourceCreateRequested = SourceCreateRequested.newBuilder()
                .setHeader(Header.newBuilder().setTime(1L).build())
                .setSourceName(sourceName)
                .setSource(buildAvroSource(sourceName, streamName))
                .build();

        ConsumerRecord<byte[], byte[]> record = sourceCreateConsumerFactory.create(SourceDaoImpl.SOURCE_COMMANDS_TOPIC_NAME,
                sourceName, sourceCreateRequested);
        topologyTestDriver.pipeInput(record);

        ProducerRecord record1 = topologyTestDriver.readOutput(SourceDaoImpl.SOURCE_ENTITY_TOPIC_NAME,
                new StringDeserializer(),
                sourceEntitySerde.deserializer());
    }

    private static com.homeaway.digitalplatform.streamregistry.Source buildAvroSource(String sourceName, String streamName) {
        Map<String, String> map = new HashMap<>();
        map.put("kinesis.url", "url");

        return com.homeaway.digitalplatform.streamregistry.Source
                .newBuilder()
                .setHeader(Header.newBuilder().setTime(1L).build())
                .setSourceName(sourceName)
                .setStreamName(streamName)
                .setSourceType("kinesis")
                .setStatus("NOT_RUNNING")
                .setTags(map)
                .setConfiguration(map)
                .build();
    }
}
