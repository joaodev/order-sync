package com.joaodev.ordersync;

import com.joaodev.ordersync.controller.dto.CreateOrderRequest;
import com.joaodev.ordersync.controller.dto.UpdateOrderRequest;
import com.joaodev.ordersync.domain.Order;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@Testcontainers
class OrderControllerIT {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart");

    @DynamicPropertySource
    static void registerDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", oracle::getJdbcUrl);
        registry.add("spring.datasource.username", oracle::getUsername);
        registry.add("spring.datasource.password", oracle::getPassword);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void createsOrderViaRestEndpoint() {
        CreateOrderRequest request = new CreateOrderRequest(
                2001L, "REST Customer", "SKU-REST", 4, new BigDecimal("30.00"), "PENDING");

        ResponseEntity<Order> response = restTemplate.postForEntity("/api/orders", request, Order.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        assertThat(response.getBody().getCurrentVersion()).isEqualTo(1);
    }

    @Test
    void updatesOrderViaRestEndpoint() {
        CreateOrderRequest createRequest = new CreateOrderRequest(
                2002L, "REST Customer 2", "SKU-REST2", 1, new BigDecimal("15.00"), "PENDING");

        ResponseEntity<Order> createResponse = restTemplate.postForEntity("/api/orders", createRequest, Order.class);

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(createResponse.getBody()).isNotNull();
        assertThat(createResponse.getBody().getLegacyOrderId()).isEqualTo(2002L);
        assertThat(createResponse.getBody().getId()).isNotNull();

        UpdateOrderRequest updateRequest = new UpdateOrderRequest(
                "REST Customer 2", "SKU-REST2", 1, new BigDecimal("15.00"), "CONFIRMED");

        ResponseEntity<Order> response = restTemplate.exchange(
                "/api/orders/2002",
                org.springframework.http.HttpMethod.PUT,
                new org.springframework.http.HttpEntity<>(updateRequest),
                Order.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getCurrentVersion()).isEqualTo(2);
        assertThat(response.getBody().getStatus()).isEqualTo("CONFIRMED");
    }

    @Test
    void creatingDuplicateOrderReturnsConflict() {
        CreateOrderRequest request = new CreateOrderRequest(
                2003L, "Dup Customer", "SKU-DUP", 1, new BigDecimal("5.00"), "PENDING");
        restTemplate.postForEntity("/api/orders", request, Order.class);

        ResponseEntity<Map> response = restTemplate.postForEntity("/api/orders", request, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void creatingOrderWithInvalidDataReturnsBadRequest() {
        CreateOrderRequest invalid = new CreateOrderRequest(
                2004L, "", "SKU-BAD", -1, new BigDecimal("-5.00"), "PENDING");

        ResponseEntity<Map> response = restTemplate.postForEntity("/api/orders", invalid, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}