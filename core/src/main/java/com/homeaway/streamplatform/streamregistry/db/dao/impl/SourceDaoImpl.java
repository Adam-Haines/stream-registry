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
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.dropwizard.lifecycle.Managed;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
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
@Slf4j
public class SourceDaoImpl implements SourceDao, Managed {


    /**
     * Source entity store name
     */
    public static final String SOURCE_ENTITY_STORE_NAME = "source-entity-store-v1";

    /**
     * The constant SOURCE_ENTITY_TOPIC_NAME.
     */
    public static final String SOURCE_ENTITY_TOPIC_NAME = "source-entity-v1";

    /**
     * Application id for the Source entity processor
     */
    public static final String SOURCE_ENTITY_PROCESSOR_APP_ID = "source-entity-processor-v1";

    /**
     * The constant PRODUCER_TOPIC_NAME.
     */
    public static final String SOURCE_COMMANDS_TOPIC = "source-command-events-v1";

    public static final String SOURCE_COMMANDS_PROCESSOR_APP_ID = "source-commands-processor-v1";

    public static final File SOURCE_COMMAND_EVENT_DIR = new File("/tmp/sourceCommands");
    public static final  File SOURCE_ENTITY_EVENT_DIR = new File("/tmp/sourceEntity");

    private final Properties commonConfig;
    private final KStreamsProcessorListener testListener;
    private boolean isRunning = false;
    private KafkaStreams sourceEntityProcessor;
    private KafkaStreams sourceCommandProcessor;
    KafkaProducer<String, SourceCreateRequested> createRequestProducer;
    KafkaProducer<String, SourceUpdateRequested> updateRequestProducer;
    KafkaProducer<String, SourceStartRequested> startRequestProducer;
    KafkaProducer<String, SourcePauseRequested> pauseRequestProducer;
    KafkaProducer<String, SourceStopRequested> stopRequestProducer;
    KafkaProducer<String, SourceResumeRequested> resumeRequestProducer;
    KafkaProducer<String, Source> deleteProducer;

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

        Properties commandProcessorConfig = new Properties();
        commonConfig.forEach(commandProcessorConfig::put);
        commandProcessorConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        commandProcessorConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        commandProcessorConfig.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        createRequestProducer = new KafkaProducer<>(commandProcessorConfig);
        updateRequestProducer = new KafkaProducer<>(commandProcessorConfig);
        startRequestProducer = new KafkaProducer<>(commandProcessorConfig);
        pauseRequestProducer = new KafkaProducer<>(commandProcessorConfig);
        stopRequestProducer = new KafkaProducer<>(commandProcessorConfig);
        resumeRequestProducer = new KafkaProducer<>(commandProcessorConfig);
        deleteProducer = new KafkaProducer(commandProcessorConfig);

    }

    @Override
    public void insert(Source source) {

        getSupportedSourceType(source);

        ProducerRecord<String, SourceCreateRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC, source.getSourceName(),
                SourceCreateRequested.newBuilder()
                        .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                        .setSourceName(source.getSourceName())
                        .setSource(modelToAvroSource(source, Status.NOT_RUNNING))
                        .build());
        Future<RecordMetadata> future = createRequestProducer.send(record);
        // Wait for the message synchronously
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error producing message", e);
        }
    }

    private void getSupportedSourceType(Source source) {
        boolean supportedSource = SOURCE_TYPES.stream()
                .anyMatch(sourceType -> sourceType.equalsIgnoreCase(source.getSourceType()));

        if (!supportedSource) {
            throw new UnsupportedSourceTypeException(source.getSourceType());
        }
    }

    @Override
    public void update(Source source) {

        ProducerRecord<String, SourceUpdateRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC, source.getSourceName(),
                SourceUpdateRequested.newBuilder()
                        .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                        .setSourceName(source.getSourceName())
                        .setSource(modelToAvroSource(source, Status.NOT_RUNNING))
                        .build());
        Future<RecordMetadata> future = updateRequestProducer.send(record);
        // Wait for the message synchronously
        try {
            future.get();
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
            ProducerRecord<String, SourceStartRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC, source.get().getSourceName(),
                    SourceStartRequested.newBuilder()
                            .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                            .setSourceName(source.get().getSourceName())
                            .setSource(modelToAvroSource(source.get(), Status.NOT_RUNNING))
                            .build());
            Future<RecordMetadata> future = startRequestProducer.send(record);
            // Wait for the message synchronously
            try {
                future.get();
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
            ProducerRecord<String, SourcePauseRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC, source.get().getSourceName(),
                    SourcePauseRequested.newBuilder()
                            .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                            .setSourceName(source.get().getSourceName())
                            .setSource(modelToAvroSource(source.get(), Status.NOT_RUNNING))
                            .build());
            Future<RecordMetadata> future = pauseRequestProducer.send(record);
            // Wait for the message synchronously
            try {
                future.get();
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
            ProducerRecord<String, SourceResumeRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC, source.get().getSourceName(),
                    SourceResumeRequested.newBuilder()
                            .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                            .setSourceName(source.get().getSourceName())
                            .setSource(modelToAvroSource(source.get(), Status.NOT_RUNNING))
                            .build());
            Future<RecordMetadata> future = resumeRequestProducer.send(record);
            // Wait for the message synchronously
            try {
                future.get();
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
            ProducerRecord<String, SourceStopRequested> record = new ProducerRecord<>(SOURCE_COMMANDS_TOPIC, source.get().getSourceName(),
                    SourceStopRequested.newBuilder()
                            .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                            .setSourceName(source.get().getSourceName())
                            .setSource(modelToAvroSource(source.get(), Status.NOT_RUNNING))
                            .build());
            Future<RecordMetadata> future = stopRequestProducer.send(record);
            // Wait for the message synchronously
            try {
                future.get();
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
        // Wait for the message synchronously
        try {
            future.get();
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


    private void initiateSourceEntityProcessor() {
        Properties sourceProcessorConfig = new Properties();
        commonConfig.forEach(sourceProcessorConfig::put);
        sourceProcessorConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, SOURCE_ENTITY_PROCESSOR_APP_ID);
        sourceProcessorConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        sourceProcessorConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        sourceProcessorConfig.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        sourceProcessorConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        sourceProcessorConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        sourceProcessorConfig.put(StreamsConfig.STATE_DIR_CONFIG, SOURCE_ENTITY_EVENT_DIR.getPath());

        //TODO: Fix deprecated KStreamBuilder to using StreamsBuilder instead
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        kStreamBuilder.globalTable(SOURCE_ENTITY_TOPIC_NAME, SOURCE_ENTITY_STORE_NAME);

        sourceEntityProcessor = new KafkaStreams(kStreamBuilder, sourceProcessorConfig);

        sourceEntityProcessor.setStateListener((newState, oldState) -> {
            if (!isRunning && newState == KafkaStreams.State.RUNNING) {
                isRunning = true;
                if (testListener != null) {
                    testListener.stateStoreInitialized();
                }
            }
        });

        sourceEntityProcessor.setUncaughtExceptionHandler((t, e) -> log.error("Source entity processor job failed", e));
        sourceEntityProcessor.start();
        log.info("Source entity processor started.");
        log.info("Source entity state Store Name: {}", SOURCE_ENTITY_STORE_NAME);
        sourceEntityStore = sourceEntityProcessor.store(SOURCE_ENTITY_STORE_NAME, QueryableStoreTypes.keyValueStore());
    }

    private void initiateSourceCommandProcessor() {

        // Pure commands, doesn't need a state store

        Properties commandProcessorConfig = new Properties();
        commonConfig.forEach(commandProcessorConfig::put);
        commandProcessorConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        commandProcessorConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        commandProcessorConfig.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        commandProcessorConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        commandProcessorConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        commandProcessorConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, SOURCE_COMMANDS_PROCESSOR_APP_ID);
        commandProcessorConfig.put(StreamsConfig.STATE_DIR_CONFIG, SOURCE_COMMAND_EVENT_DIR.getPath());

        sourceCommandBuilder();

        sourceCommandProcessor = new KafkaStreams(sourceCommandBuilder().build(), commandProcessorConfig);
        sourceCommandProcessor.setStateListener((newState, oldState) -> {
            if (!isRunning && newState == KafkaStreams.State.RUNNING) {
                isRunning = true;
                if (testListener != null) {
                    testListener.stateStoreInitialized();
                }
            }
        });

        sourceCommandProcessor.setUncaughtExceptionHandler((t, e) -> log.error("Source command processor job failed", e));
        sourceCommandProcessor.start();
        log.info("Source commands processor job started.");

    }

    private StreamsBuilder sourceCommandBuilder() {

        StreamsBuilder sourceCommandBuilder = new StreamsBuilder();

        sourceCommandBuilder
                .stream(SOURCE_COMMANDS_TOPIC)
                .map((sourceName, command) ->
                        new ProcessRecord().process(command))
                .to(SOURCE_ENTITY_TOPIC_NAME);

        return sourceCommandBuilder;
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

    private KeyValue<String, com.homeaway.digitalplatform.streamregistry.Source> setNewStatus(com.homeaway.digitalplatform.streamregistry.Source source, Status status) {
        return new KeyValue<>(source.getSourceName(), com.homeaway.digitalplatform.streamregistry.Source.newBuilder()
                .setHeader(Header.newBuilder().setTime(System.currentTimeMillis()).build())
                .setSourceName(source.getSourceName())
                .setStreamName(source.getStreamName())
                .setSourceType(source.getSourceType())
                .setStatus(status.toString())
                .setImperativeConfiguration(source.getImperativeConfiguration())
                .setTags(source.getTags())
                .build());
    }

    @Override
    public void start() {
        initiateSourceEntityProcessor();
        initiateSourceCommandProcessor();
    }


    @Override
    public void stop() {
        sourceCommandProcessor.close();
        sourceEntityProcessor.close();
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
                .imperativeConfiguration(avroSource.getImperativeConfiguration())
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
                .setImperativeConfiguration(source.getImperativeConfiguration())
                .setTags(source.getTags())
                .build();
    }

    private class ProcessRecord<V> {

        ProcessRecord() {
        }

        KeyValue process(V entity) {
            if (entity instanceof SourceCreateRequested) {
                return setNewStatus(((SourceCreateRequested) entity)
                        .getSource(), Status.NOT_RUNNING);
            } else if (entity instanceof SourceStartRequested) {
                return setNewStatus(((SourceStartRequested) entity)
                        .getSource(), Status.STARTING);
            } else if (entity instanceof SourceUpdateRequested) {
                return setNewStatus(((SourceUpdateRequested) entity)
                        .getSource(), Status.UPDATING);
            } else if (entity instanceof SourcePauseRequested) {
                return setNewStatus(((SourcePauseRequested) entity)
                        .getSource(), Status.PAUSING);
            } else if (entity instanceof SourceResumeRequested) {
                return setNewStatus(((SourceResumeRequested) entity)
                        .getSource(), Status.RESUMING);
            } else if (entity instanceof SourceStopRequested) {
                return setNewStatus(((SourceStopRequested) entity)
                        .getSource(), Status.STOPPING);
            }
            return null;
        }
    }

}