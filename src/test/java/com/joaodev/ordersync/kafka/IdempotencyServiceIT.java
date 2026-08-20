package com.joaodev.ordersync.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
public class IdempotencyServiceIT {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void registerRedisPropertis(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private IdempotencyService idempotencyService;

    @Test
    void firstOccurrenceOfEventIsNew() {
        assertThat(idempotencyService.isNewEvent("scn-1001")).isTrue();
    }

    @Test
    void secondOccurrenceOfSameEventIsNotNew() {
        idempotencyService.isNewEvent("scn-2002");

        assertThat(idempotencyService.isNewEvent("scn-2002")).isFalse();
    }
}
