package com.joaodev.ordersync.kafka;

import com.joaodev.ordersync.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
public class OrderCdcListenerIT {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.0");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", oracle::getJdbcUrl);
        registry.add("spring.datasource.username", oracle::getUsername);
        registry.add("spring.datasource.password", oracle::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.producer.key-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("spring.kafka.producer.value-serializer",
                () -> "org.apache.kafka.common.serialization.StringSerializer");
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void consumesDebeziumCreateEventAndPersistsOrder() {
        String payload = """
                {
                    "payload": {
                        "before": null,
                        "after": {
                            "ORDER_ID": {"scale":0,"value":"AQ=="},
                            "CUSTOMER_NAME": "Kafka Test Customer",
                            "PRODUCT_CODE": "SKU-KAFKA",
                            "QUANTITY": {"scale":0,"value":"Ag=="},
                            "UNIT_PRICE": "E34=",
                            "STATUS": "PENDING"
                        },
                        "op": "c"
                    }
                }
                """;

        kafkaTemplate.send("ordersync.ORDER_SYNC.LEGACY_ORDERS", payload);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(orderRepository.findByLegacyOrderId(1L)).isPresent());

        var order = orderRepository.findByLegacyOrderId(1L).orElseThrow();
        assertThat(order.getCustomerName()).isEqualTo("Kafka Test Customer");
        assertThat(order.getUnitPrice()).isEqualByComparingTo("49.90");
    }
}
