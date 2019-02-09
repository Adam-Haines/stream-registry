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

import static org.hamcrest.core.Is.is;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.db.dao.impl.SourceDaoImpl;
import com.homeaway.streamplatform.streamregistry.model.Source;

public class SourceResourceIT extends BaseResourceIT {

    // SourceImpl has a lot many processors.
    // Takes longer for messages to show up in the consumer.
    public static final int SOURCE_WAIT_TIME_MS = 4000;


    public static Properties commonConfig;

    private static SourceDao sourceDao;



    @BeforeClass
    public static void setUp() throws Exception {

        // Make sure all temp dirs are cleaned first
        // This will solve a lot of the dir locked issue etc.
        FileUtils.deleteDirectory(SourceDaoImpl.SOURCE_COMMAND_EVENT_DIR);
        FileUtils.deleteDirectory(SourceDaoImpl.SOURCE_ENTITY_EVENT_DIR);

        commonConfig = new Properties();
        commonConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        commonConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        commonConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        commonConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());

        CompletableFuture<Boolean> initialized = new CompletableFuture<>();
        sourceDao = new SourceDaoImpl(commonConfig,  () -> initialized.complete(true));
        sourceDao.start();
    }

    @Test
    public void testSourceDaoImpl() throws Exception {

        String sourceName = "source-a";
        String streamName = "stream-a";

        Assert.assertNotNull(commonConfig);

        Source source = buildSource(sourceName, streamName, null);

        sourceDao.insert(source);

        // Seems to need longer time. Have more things to setup
        Thread.sleep(SOURCE_WAIT_TIME_MS);

        Optional<Source> optionalSource = sourceDao.get(sourceName);

        Assert.assertThat(optionalSource.isPresent(), is(true));

        Assert.assertThat("Get source should return the source that was inserted",
                optionalSource.get().toString() , is(buildSource(sourceName, streamName, "NOT_RUNNING").toString()));

        Assert.assertThat(sourceDao.getStatus(sourceName), is("NOT_RUNNING"));

        sourceDao.start(sourceName);

        Thread.sleep(SOURCE_WAIT_TIME_MS);

        Assert.assertThat(sourceDao.getStatus(sourceName), is("STARTING"));

        sourceDao.pause(sourceName);

        Thread.sleep(SOURCE_WAIT_TIME_MS);

        Assert.assertThat(sourceDao.getStatus(sourceName), is("PAUSING"));

        sourceDao.resume(sourceName);

        Thread.sleep(SOURCE_WAIT_TIME_MS);

        Assert.assertThat(sourceDao.getStatus(sourceName), is("RESUMING"));

        sourceDao.stop(sourceName);

        Thread.sleep(SOURCE_WAIT_TIME_MS);

        Assert.assertThat(sourceDao.getStatus(sourceName), is("STOPPING"));
    }

    private Source buildSource(String sourceName, String streamName, String status) {

        Map<String, String> map = new HashMap<>();
        map.put("kinesis.url", "url");

        return Source.builder()
                .sourceName(sourceName)
                .streamName(streamName)
                .sourceType("kinesis")
                .status(status)
                .imperativeConfiguration(map)
                .tags(map)
                .build();
    }
}
