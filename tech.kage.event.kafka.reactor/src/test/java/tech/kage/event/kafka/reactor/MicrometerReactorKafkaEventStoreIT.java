/*
 * Copyright (c) 2024-2025, Dariusz Szpakowski
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package tech.kage.event.kafka.reactor;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import tech.kage.event.Event;

/**
 * Micrometer integration tests for {@link ReactorKafkaEventStore}.
 * 
 * @author Dariusz Szpakowski
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext
class MicrometerReactorKafkaEventStoreIT {
    // UUT
    @Autowired
    ReactorKafkaEventStore eventStore;

    @Autowired
    DatabaseClient databaseClient;

    @Autowired
    SenderOptions<UUID, SpecificRecord> kafkaSenderOptions;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    TransactionalOperator transactionalOperator;

    String topic;

    static final Network network = Network.newNetwork();

    @SuppressWarnings("resource")
    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1").withNetwork(network);

    @SuppressWarnings("resource")
    @Container
    static final GenericContainer<?> schemaRegistry = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
            .dependsOn(kafka)
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                    "PLAINTEXT://%s:9092".formatted(kafka.getNetworkAliases().get(0)))
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        registry.add(
                "spring.kafka.properties.schema.registry.url",
                () -> "http://%s:%s".formatted(schemaRegistry.getHost(), schemaRegistry.getFirstMappedPort()));
    }

    @Configuration
    @EnableAutoConfiguration
    @Import(ReactorKafkaEventStore.class)
    static class TestConfiguration {
        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @BeforeEach
    void setUp(@Value("classpath:/test-data/events/ddl.sql") Resource ddl) throws IOException {
        topic = "test_" + System.currentTimeMillis() + "_events";

        databaseClient
                .sql(ddl.getContentAsString(StandardCharsets.UTF_8))
                .fetch()
                .rowsUpdated()
                .block();
    }

    @Test
    void registersConsumerLagGauges() {
        // Given
        var partitionCount = 4;

        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).partitions(partitionCount).build());

        var events = IntStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> TestPayload.newBuilder().setText("test payload " + id).build())
                .map((SpecificRecord payload) -> Event.from(UUID.randomUUID(), payload))
                .toList();

        Flux.concat(events.stream().map(event -> eventStore.save(topic, event)).toList()).blockLast();

        // When
        eventStore
                .subscribe(topic)
                .concatMap(event -> event.as(transactionalOperator::transactional))
                .take(3)
                .timeout(Duration.ofSeconds(60))
                .blockLast();

        // Then
        var consumerLagGauges = meterRegistry
                .find("event.store.consumer.lag")
                .tags("topic", topic)
                .gauges();

        assertThat(consumerLagGauges)
                .describedAs("consumer lag gauges count")
                .hasSize(partitionCount);
    }

    @Test
    void clearsOutdatedConsumerLagGauges() {
        // Given
        meterRegistry.gauge("event.store.consumer.lag", List.of(Tag.of("topic", topic), Tag.of("partition", "1")), 123);
        meterRegistry.gauge("event.store.consumer.lag", List.of(Tag.of("topic", topic), Tag.of("partition", "3")), 123);
        meterRegistry.gauge("event.store.consumer.lag", List.of(Tag.of("topic", topic), Tag.of("partition", "9")), 123);

        var partitionCount = 2;
        var expectedPartitions = List.of("0", "1");

        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).partitions(partitionCount).build());

        var events = IntStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> TestPayload.newBuilder().setText("test payload " + id).build())
                .map((SpecificRecord payload) -> Event.from(UUID.randomUUID(), payload))
                .toList();

        Flux.concat(events.stream().map(event -> eventStore.save(topic, event)).toList()).blockLast();

        // When
        eventStore
                .subscribe(topic)
                .concatMap(event -> event.as(transactionalOperator::transactional))
                .take(3)
                .timeout(Duration.ofSeconds(60))
                .blockLast();

        // Then
        var consumerLagGaugesPartitions = meterRegistry
                .find("event.store.consumer.lag")
                .tags("topic", topic)
                .gauges()
                .stream()
                .map(gauge -> gauge.getId().getTag("partition"))
                .toList();

        assertThat(consumerLagGaugesPartitions)
                .describedAs("consumer lag gauges partitions")
                .hasSameElementsAs(expectedPartitions);
    }

    @Test
    void computesConsumerLag() {
        // Given
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).partitions(1).build());

        var events = IntStream
                .rangeClosed(1, 10)
                .boxed()
                .map(id -> TestPayload.newBuilder().setText("test payload " + id).build())
                .map((SpecificRecord payload) -> Event.from(UUID.randomUUID(), payload))
                .toList();

        var kafkaSender = KafkaSender.create(
                kafkaSenderOptions.producerProperty(
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, "MicrometerReactorKafkaTest"));

        for (var event : events) {
            kafkaSender.sendTransactionally(
                    Flux.just(
                            Mono.just(
                                    SenderRecord.create(
                                            topic,
                                            null,
                                            event.timestamp().toEpochMilli(),
                                            event.key(),
                                            event.payload(),
                                            null))))
                    .blockLast();
        }

        var expectedConsumerLag = 14;

        // When
        eventStore
                .subscribe(topic)
                .concatMap(event -> event.as(transactionalOperator::transactional))
                .take(3)
                .timeout(Duration.ofSeconds(60))
                .blockLast();

        // Then
        var consumerLag = meterRegistry
                .find("event.store.consumer.lag")
                .tags("topic", topic, "partition", "0")
                .gauge()
                .value();

        assertThat(consumerLag)
                .describedAs("consumer lag")
                .isEqualTo(expectedConsumerLag);
    }
}
