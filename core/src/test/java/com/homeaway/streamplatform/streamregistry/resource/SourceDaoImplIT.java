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

import com.homeaway.digitalplatform.streamregistry.Header;
import com.homeaway.digitalplatform.streamregistry.SourceCreateRequested;
import com.homeaway.digitalplatform.streamregistry.SourcePauseRequested;
import com.homeaway.digitalplatform.streamregistry.SourceResumeRequested;
import com.homeaway.digitalplatform.streamregistry.SourceStartRequested;
import com.homeaway.digitalplatform.streamregistry.SourceStopRequested;
import com.homeaway.digitalplatform.streamregistry.SourceUpdateRequested;
import com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.model.SourceType;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class SourceDaoImplIT {

    private static SourceDaoImpl sourceDao;

    private TopologyTestDriver sourceCommandDriver;
    private TopologyTestDriver sourceEntityDriver;
    private SchemaRegistryClient mockSchemaRegistryClient;


    @Before
    public void setUp() throws Exception {

        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        mockSchemaRegistryClient.register("source-command-events-v1-com.homeaway.digitalplatform.streamregistry.SourceCreateRequested", SourceCreateRequested.SCHEMA$);
        mockSchemaRegistryClient.register("source-command-events-v1-com.homeaway.digitalplatform.streamregistry.SourceUpdateRequested", SourceUpdateRequested.SCHEMA$);
        mockSchemaRegistryClient.register("source-command-events-v1-com.homeaway.digitalplatform.streamregistry.SourceStartRequested", SourceStartRequested.SCHEMA$);
        mockSchemaRegistryClient.register("source-command-events-v1-com.homeaway.digitalplatform.streamregistry.SourcePauseRequested", SourcePauseRequested.SCHEMA$);
        mockSchemaRegistryClient.register("source-command-events-v1-com.homeaway.digitalplatform.streamregistry.SourceResumeRequested", SourceResumeRequested.SCHEMA$);
        mockSchemaRegistryClient.register("source-command-events-v1-com.homeaway.digitalplatform.streamregistry.SourceStopRequested", SourceStopRequested.SCHEMA$);


        Properties commonConfig = new Properties();
        commonConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        commonConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy:8080");
        commonConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "something-new");
        sourceDao = new SourceDaoImpl(commonConfig, null);

        sourceCommandDriver = new TopologyTestDriver(sourceDao.getSourceCommandTopology(), commonConfig);
        sourceEntityDriver = new TopologyTestDriver(sourceDao.getSourceEntityTopology(), commonConfig);
    }


    @Test
    public void testTopology() {

        SpecificAvroSerde<com.homeaway.digitalplatform.streamregistry.SourceCreateRequested> createRequestedSerde =
                new SpecificAvroSerde<>(mockSchemaRegistryClient);

        Map<String, String> config = Collections.singletonMap("schema.registry.url", "http://dummy:8080");

        createRequestedSerde.configure(config, false);

        @SuppressWarnings("unchecked")
        ConsumerRecordFactory<String, com.homeaway.digitalplatform.streamregistry.SourceCreateRequested> sourceCreateConsumerFactory =
                new ConsumerRecordFactory(SourceDaoImpl.SOURCE_COMMANDS_TOPIC,
                new StringSerializer(), createRequestedSerde.serializer());

        com.homeaway.digitalplatform.streamregistry.SourceCreateRequested sourceCreateRequested = com.homeaway.digitalplatform.streamregistry.SourceCreateRequested.newBuilder()
                .setHeader(Header.newBuilder().setTime(1L).build())
                .setSourceName("source-a")
                .setSource(buildAvroSource("streamA", "sourceA", "NOT_RUNNING"))
                .build();

        sourceCommandDriver.pipeInput(sourceCreateConsumerFactory.create(SourceDaoImpl.SOURCE_COMMANDS_TOPIC, "sourceA", sourceCreateRequested));

        SpecificAvroSerde<com.homeaway.digitalplatform.streamregistry.Source> sourceEntitySerde =
                new SpecificAvroSerde<>(mockSchemaRegistryClient);

        sourceEntitySerde.configure(config, false);

        ProducerRecord record1 = sourceCommandDriver.readOutput(SourceDaoImpl.SOURCE_ENTITY_TOPIC_NAME, new StringDeserializer(),
                sourceEntitySerde.deserializer());




    }

    private static com.homeaway.digitalplatform.streamregistry.Source buildAvroSource(String sourceName, String streamName, String status) {
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
                .setImperativeConfiguration(map)
                .build();
    }

    class SpecificAvro extends SpecificAvroSerde {
    }


}
