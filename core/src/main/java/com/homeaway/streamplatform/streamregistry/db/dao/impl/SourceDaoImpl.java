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
package com.homeaway.streamplatform.streamregistry.db.dao.impl;

import static com.homeaway.streamplatform.streamregistry.model.SourceType.SOURCE_TYPES;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.inject.Singleton;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.dropwizard.lifecycle.Managed;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.homeaway.digitalplatform.streamregistry.Header;
import com.homeaway.digitalplatform.streamregistry.SourceCreateRequested;
import com.homeaway.digitalplatform.streamregistry.SourcePauseRequested;
import com.homeaway.digitalplatform.streamregistry.SourceResumeRequested;
import com.homeaway.digitalplatform.streamregistry.SourceStartRequested;
import com.homeaway.digitalplatform.streamregistry.SourceStopRequested;
import com.homeaway.digitalplatform.streamregistry.SourceUpdateRequested;
import com.homeaway.streamplatform.streamregistry.db.dao.SourceDao;
import com.homeaway.streamplatform.streamregistry.exceptions.SourceNotFoundException;
import com.homeaway.streamplatform.streamregistry.exceptions.UnsupportedSourceTypeException;
import com.homeaway.streamplatform.streamregistry.model.Source;
import com.homeaway.streamplatform.streamregistry.streams.KStreamsProcessorListener;


/**
 * KStreams event processor implementation of the SourceDao
 * All calls to this Dao represent asynchronous/eventually consistent actions.
 */
@Singleton
@Slf4j
public class SourceDaoImpl implements SourceDao, Managed {
    /** Application id for the Source entity processor */
    public static final String SOURCE_ENTITY_PROCESSOR_APP_ID = "source-entity-processor-v1";

    /** Source entity store name */
    public static final String SOURCE_ENTITY_STORE_NAME = "source-entity-store-v1";

    /** Source entity topic name */
    public static final String SOURCE_ENTITY_TOPIC_NAME = "source-entity-v1";

    /** Source command topic name */
    public static final String SOURCE_COMMANDS_TOPIC_NAME = "source-command-events-v1";

    /** Source processor dir name */
    public static final String SOURCE_PROCESSOR_DIRNAME = "/tmp/stream-registry/streams/sourceEntity";

    private static final File SOURCE_PROCESSOR_DIR = new File(SOURCE_PROCESSOR_DIRNAME);

    private final Properties commonConfig;
    private final KStreamsProcessorListener testListener;
    private boolean isRunning = false;


    @Getter
    private KafkaStreams sourceProcessor;

    private GlobalKTable<String, com.homeaway.digitalplatform.streamregistry.Source> sourceEntityKTable;

    private KafkaProducer<String, SourceCreateRequested> createRequestProducer;
    private KafkaProducer<String, SourceUpdateRequested> updateRequestProducer;
    private KafkaProducer<String, SourceStartRequested> startRequestProducer;
    private KafkaProducer<String, SourcePauseRequested> pauseRequestProducer;
    private KafkaProducer<String, SourceStopRequested> stopRequestProducer;
    private KafkaProducer<String, SourceResumeRequested> resumeRequestProducer;
    private KafkaProducer<String, Source> deleteProducer;

    @Getter
    private ReadOnlyKeyValueStore<String, com.homeaway.digitalplatform.streamregistry.Source> sourceEntityStore;

    /**
     * Instantiates a new Source dao.
     *
     * @param commonConfig the common config
     */
    public SourceDaoImpl(Properties commonConfig) {
        this(commonConfig, null);
    }

    /**
     * Instantiates a new Source dao.
     *
     * @param commonConfig the common config
     * @param testListener the test listener
     */
    public SourceDaoImpl(Properties commonConfig, KStreamsProcessorListener testListener) {
        this.commonConfig = commonConfig;
        this.testListener = testListener;
    }

    @Override
    public void insert(Source source) {

        validateSourceIsSupported(source);

        ProducerRecord<String, SourceCreateRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC_NAME, source.getSourceName(),
                SourceCreateRequested.newBuilder()
                        .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                        .setSourceName(source.getSourceName())
                        .setSource(modelToAvroSource(source, Status.NOT_RUNNING))
                        .build());
        Future<RecordMetadata> future = createRequestProducer.send(record);
        // Wait for the message synchronously
        try {
            future.get();
            log.info("inserting - {}", source.getSourceName());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error producing message", e);
        }
    }

    private void validateSourceIsSupported(Source source) {
        boolean supportedSource = SOURCE_TYPES.stream()
                .anyMatch(sourceType -> sourceType.equalsIgnoreCase(source.getSourceType()));

        if (!supportedSource) {
            throw new UnsupportedSourceTypeException(source.getSourceType());
        }
    }

    @Override
    public void update(Source source) {

        ProducerRecord<String, SourceUpdateRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC_NAME, source.getSourceName(),
                SourceUpdateRequested.newBuilder()
                        .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                        .setSourceName(source.getSourceName())
                        .setSource(modelToAvroSource(source, Status.NOT_RUNNING))
                        .build());
        Future<RecordMetadata> future = updateRequestProducer.send(record);
        updateRequestProducer.flush();
        // Wait for the message synchronously
        try {
            future.get();
            log.info("updating - {}", source.getSourceName());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error producing message", e);
        }
    }

    @Override
    public Optional<Source> get(String sourceName) {
        return Optional.ofNullable(avroToModelSource(sourceEntityStore.get(sourceName)));
    }

    @Override
    public void start(String sourceName) throws SourceNotFoundException {

        Optional<Source> source = get(sourceName);
        if (source.isPresent()) {
            ProducerRecord<String, SourceStartRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC_NAME, source.get().getSourceName(),
                    SourceStartRequested.newBuilder()
                            .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                            .setSourceName(source.get().getSourceName())
                            .build());
            Future<RecordMetadata> future = startRequestProducer.send(record);
            startRequestProducer.flush();

            // Wait for the message synchronously
            try {
                future.get();
                log.info("starting - {}", sourceName);
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error producing message", e);
            }
        } else {
            throw new SourceNotFoundException(sourceName);
        }
    }

    @Override
    public void pause(String sourceName) throws SourceNotFoundException {

        Optional<Source> source = get(sourceName);
        if (source.isPresent()) {
            ProducerRecord<String, SourcePauseRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC_NAME, source.get().getSourceName(),
                    SourcePauseRequested.newBuilder()
                            .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                            .setSourceName(source.get().getSourceName())
                            .build());
            Future<RecordMetadata> future = pauseRequestProducer.send(record);
            pauseRequestProducer.flush();

            // Wait for the message synchronously
            try {
                future.get();
                log.info("pausing - {}", sourceName);
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error producing message", e);
            }
        } else {
            throw new SourceNotFoundException(sourceName);
        }

    }

    @Override
    public void resume(String sourceName) throws SourceNotFoundException {
        Optional<Source> source = get(sourceName);
        if (source.isPresent()) {
            ProducerRecord<String, SourceResumeRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC_NAME, source.get().getSourceName(),
                    SourceResumeRequested.newBuilder()
                            .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                            .setSourceName(source.get().getSourceName())
                            .build());
            Future<RecordMetadata> future = resumeRequestProducer.send(record);
            resumeRequestProducer.flush();
            // Wait for the message synchronously
            try {
                future.get();
                log.info("resuming - {}", sourceName);
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error producing message", e);
            }
        } else {
            throw new SourceNotFoundException(sourceName);
        }
    }

    @Override
    public void stop(String sourceName) throws SourceNotFoundException {
        Optional<Source> source = get(sourceName);
        if (source.isPresent()) {
            ProducerRecord<String, SourceStopRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC_NAME, source.get().getSourceName(),
                    SourceStopRequested.newBuilder()
                            .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                            .setSourceName(source.get().getSourceName())
                            .build());
            Future<RecordMetadata> future = stopRequestProducer.send(record);
            stopRequestProducer.flush();
            // Wait for the message synchronously
            try {
                future.get();
                log.info("stopping - {}", sourceName);
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error producing message", e);
            }
        } else {
            throw new SourceNotFoundException(sourceName);
        }
    }

    @Override
    public String getStatus(String sourceName) {
        Optional<com.homeaway.digitalplatform.streamregistry.Source> source =
                Optional.ofNullable(sourceEntityStore.get(sourceName));

        if (!source.isPresent()) {
            throw new SourceNotFoundException(sourceName);
        }
        return source.get().getStatus();
    }

    @Override
    public void delete(String sourceName) {
        ProducerRecord<String, Source> record = new ProducerRecord<>(SOURCE_ENTITY_TOPIC_NAME, sourceName, null);
        Future<RecordMetadata> future = deleteProducer.send(record);

        deleteProducer.flush();

        // Wait for the message synchronously
        try {
            future.get();
            log.info("deleting - {}", sourceName);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error producing message", e);
        }

    }

    @Override
    public List<Source> getAll() {
        List<Source> sources = new ArrayList<>();
        KeyValueIterator<String, com.homeaway.digitalplatform.streamregistry.Source> iterator =
                sourceEntityStore.all();
        iterator.forEachRemaining(keyValue -> sources.add(avroToModelSource(keyValue.value)));
        return sources;
    }


    private void initiateProcessor() {
        Properties sourceProcessorConfig = new Properties();
        commonConfig.forEach(sourceProcessorConfig::put);
        sourceProcessorConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, SOURCE_ENTITY_PROCESSOR_APP_ID);
        sourceProcessorConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        sourceProcessorConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        sourceProcessorConfig.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        sourceProcessorConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        sourceProcessorConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        sourceProcessorConfig.put(StreamsConfig.STATE_DIR_CONFIG, SOURCE_PROCESSOR_DIR.getPath());
        sourceProcessorConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        sourceProcessorConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");


        final Map<String, String> serdeConfig =
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        commonConfig.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));

        final Serde<SpecificRecord> commandEventSerde = new SpecificAvroSerde<>();
        commandEventSerde.configure(serdeConfig, false);

        final Serde<com.homeaway.digitalplatform.streamregistry.Source> sourceSpecificAvroSerde = new SpecificAvroSerde<>();
        sourceSpecificAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        declareStreamProcessor(builder, commandEventSerde, sourceSpecificAvroSerde);

        sourceProcessor = new KafkaStreams(builder.build(), sourceProcessorConfig);

        sourceProcessor.setStateListener((newState, oldState) -> {
            if (!isRunning && newState == KafkaStreams.State.RUNNING) {
                isRunning = true;
                if (testListener != null) {
                    testListener.stateStoreInitialized();
                }
            }
        });

        sourceProcessor.setUncaughtExceptionHandler((t, e) -> log.error("Source entity processor job failed", e));
        sourceProcessor.start();
        log.info("Topology started with properties - {}", sourceProcessorConfig);
        log.info("Source entity state Store Name: {}", SOURCE_ENTITY_STORE_NAME);
        sourceEntityStore = sourceProcessor.store(sourceEntityKTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());

    }

    /**
     * Declares the stream topology and declares the corresponding GlobalKTable
     * @param builder Empty StreamsBuilder instance
     * @param specificRecordSerde The Serde to use for the command events
     * @param sourceSerde The Serde to use for the global ktable state store
     */
    public void declareStreamProcessor(StreamsBuilder builder, Serde<SpecificRecord> specificRecordSerde,
                                       Serde<com.homeaway.digitalplatform.streamregistry.Source> sourceSerde) {
        Serde<String> keySerde = Serdes.String();

        // first declare our stream with command events as the input, note we specify our serdes here
        KStream<String, SpecificRecord> kstream = builder.stream(SOURCE_COMMANDS_TOPIC_NAME, Consumed.with(keySerde, specificRecordSerde));

        // now create our source entities (with compacted topic)
        kstream.map((sourceName, command) -> new ProcessRecord<SpecificRecord>().process(command))
                .to(SOURCE_ENTITY_TOPIC_NAME, Produced.with(keySerde, sourceSerde));

        // finally bind a store to the source entity topic
        sourceEntityKTable = builder.globalTable(SOURCE_ENTITY_TOPIC_NAME, Consumed.with(keySerde, sourceSerde));
    }

    public enum Status {
        NOT_RUNNING("NOT_RUNNING"),
        STARTING("STARTING"),
        UPDATING("UPDATING"),
        PAUSING("PAUSING"),
        RESUMING("RESUMING"),
        STOPPING("STOPPING");

        private final String status;

        /**
         * @param status string
         */
        Status(final String status) {
            this.status = status;
        }

        @Override
        public String toString() {
            return status;
        }
    }

    private KeyValue<String, com.homeaway.digitalplatform.streamregistry.Source> getSourceKeyValue(com.homeaway.digitalplatform.streamregistry.Source source, Status status) {
        return new KeyValue<>(source.getSourceName(), com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                .setSourceName(source.getSourceName())
                .setStreamName(source.getStreamName())
                .setSourceType(source.getSourceType())
                .setStatus(status.toString())
                .setConfiguration(source.getConfiguration())
                .setTags(source.getTags())
                .build());
    }

    private KeyValue<String, com.homeaway.digitalplatform.streamregistry.Source> getSourceKeyValueForExistingSource(String sourceName, Status status) {

        Optional<Source> optionalExistingSource = get(sourceName);
        if (!optionalExistingSource.isPresent()) {
            return null;
        }

        Source existingSource = optionalExistingSource.get();

        com.homeaway.digitalplatform.streamregistry.Source avroSource = new com.homeaway.digitalplatform.streamregistry.Source();
        avroSource.setSourceName(existingSource.getSourceName());
        avroSource.setStreamName(existingSource.getStreamName());
        avroSource.setSourceType(existingSource.getSourceType());
        avroSource.setConfiguration(existingSource.getConfiguration());
        avroSource.setTags(existingSource.getTags());

        return getSourceKeyValue(avroSource, status);
    }

    @Override
    public void start() {
        initiateProcessor();

        Properties producerConfig = new Properties();
        commonConfig.forEach(producerConfig::put);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        producerConfig.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());

        createRequestProducer = new KafkaProducer<>(producerConfig);
        updateRequestProducer = new KafkaProducer<>(producerConfig);
        startRequestProducer = new KafkaProducer<>(producerConfig);
        pauseRequestProducer = new KafkaProducer<>(producerConfig);
        stopRequestProducer = new KafkaProducer<>(producerConfig);
        resumeRequestProducer = new KafkaProducer<>(producerConfig);
        deleteProducer = new KafkaProducer<>(producerConfig);

        log.info("All the producers were initiated with the following common configuration - {}", producerConfig);
    }


    @Override
    public void stop() {
        sourceProcessor.close();
        createRequestProducer.close();
        updateRequestProducer.close();
        deleteProducer.close();
        log.info("Source command processor closed");
        log.info("Source entity processor closed");
        log.info("Source create producer closed");
        log.info("Source update producer closed");
        log.info("Source delete producer closed");
    }

    private Source avroToModelSource(com.homeaway.digitalplatform.streamregistry.Source avroSource) {
        return Source.builder()
                .sourceName(avroSource.getSourceName())
                .sourceType(avroSource.getSourceType())
                .streamName(avroSource.getStreamName())
                .status(avroSource.getStatus())
                .created(avroSource.getHeader().getTime())
                .configuration(avroSource.getConfiguration())
                .tags(avroSource.getTags())
                .build();
    }

    private com.homeaway.digitalplatform.streamregistry.Source modelToAvroSource(Source source, Status status) {
        return com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                .setSourceName(source.getSourceName())
                .setSourceType(source.getSourceType())
                .setStreamName(source.getStreamName())
                .setStatus(status.toString())
                .setConfiguration(source.getConfiguration())
                .setTags(source.getTags())
                .build();
    }

    private class ProcessRecord<V> {

        ProcessRecord() {
        }

        KeyValue process(V entity) {
            if (entity instanceof SourceCreateRequested) {
                return getSourceKeyValue(((SourceCreateRequested) entity).getSource(),
                        Status.NOT_RUNNING);
            } else if (entity instanceof SourceUpdateRequested) {
                return getSourceKeyValue(((SourceUpdateRequested) entity).getSource(),
                        Status.UPDATING);
            } else if (entity instanceof SourceStartRequested) {
                return getSourceKeyValueForExistingSource(((SourceStartRequested) entity).getSourceName(),
                        Status.STARTING);
            } else if (entity instanceof SourcePauseRequested) {
                return getSourceKeyValueForExistingSource(((SourcePauseRequested) entity).getSourceName(),
                        Status.PAUSING);
            } else if (entity instanceof SourceResumeRequested) {
                return getSourceKeyValueForExistingSource(((SourceResumeRequested) entity).getSourceName(),
                        Status.RESUMING);
            } else if (entity instanceof SourceStopRequested) {
                return getSourceKeyValueForExistingSource(((SourceStopRequested) entity).getSourceName(),
                        Status.STOPPING);
            } else {
                throw new RuntimeException("Unsupported command type for source");
            }
        }
    }

}