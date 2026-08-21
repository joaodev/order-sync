package com.joaodev.ordersync;

import com.joaodev.ordersync.controller.dto.CreateOrderRequest;
import com.joaodev.ordersync.controller.dto.UpdateOrderRequest;
import com.joaodev.ordersync.domain.Order;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
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

    @Value("${app.security.keycloak.token-uri}")
    private String tokenUri;

    @Value("${app.security.keycloak.client-id}")
    private String clientId;

    @Value("${app.security.keycloak.client-secret}")
    private String clientSecret;

    @Value("${app.security.keycloak.test-username}")
    private String testUsername;

    @Value("${app.security.keycloak.test-password}")
    private String testPassword;

    private static String cachedAccessToken;

    @BeforeEach
    void ensureAuthenticated() {
        if (cachedAccessToken == null) {
            cachedAccessToken = fetchAccessToken();
        }
    }

    private String fetchAccessToken() {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "password");
        form.add("client_id", clientId);
        form.add("client_secret", clientSecret);
        form.add("username", testUsername);
        form.add("password", testPassword);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        ResponseEntity<Map> tokenResponse = restTemplate.getRestTemplate()
                .postForEntity(tokenUri, new HttpEntity<>(form, headers), Map.class);

        Map body = tokenResponse.getBody();
        if (body == null || !body.containsKey("access_token")) {
            throw new IllegalStateException("Failed to obtain access token from Keycloak: " + body);
        }

        return (String) body.get("access_token");
    }

    private HttpHeaders authHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(cachedAccessToken);
        return headers;
    }

    @Test
    void createsOrderViaRestEndpoint() {
        CreateOrderRequest request = new CreateOrderRequest(
                2001L,
                "REST Customer",
                "SKU-REST",
                4,
                new BigDecimal("30.00"),
                "PENDING");

        ResponseEntity<Order> response = restTemplate.exchange(
                "api/orders", HttpMethod.POST, new HttpEntity<>(request, authHeaders()), Order.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        assertThat(response.getBody().getCurrentVersion()).isEqualTo(1);
    }

    @Test
    void updatesOrderViaRestEndpoint() {
        CreateOrderRequest createRequest = new CreateOrderRequest(
                2002L,
                "REST Customer 2",
                "SKU-REST2",
                1,
                new BigDecimal("15.00"),
                "PENDING");

        restTemplate.exchange(
                "/api/orders", HttpMethod.POST, new HttpEntity<>(createRequest, authHeaders()), Order.class);

        UpdateOrderRequest updateRequest = new UpdateOrderRequest(
                "REST Customer 2",
                "SKU-REST2",
                1,
                new BigDecimal("15.00"),
                "CONFIRMED");

        ResponseEntity<Order> response = restTemplate.exchange(
                "/api/orders/2002", HttpMethod.PUT, new HttpEntity<>(updateRequest, authHeaders()), Order.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getCurrentVersion()).isEqualTo(2);
        assertThat(response.getBody().getStatus()).isEqualTo("CONFIRMED");
    }

    @Test
    void creatingDuplicateOrderReturnsConflict() {
        CreateOrderRequest request = new CreateOrderRequest(
                2003L,
                "Dup Customer",
                "SKU-DUP",
                1,
                new BigDecimal("5.00"),
                "PENDING");

        restTemplate.exchange(
                "/api/orders", HttpMethod.POST, new HttpEntity<>(request, authHeaders()), Order.class);

        ResponseEntity<Map> response = restTemplate.exchange(
                "/api/orders", HttpMethod.POST, new HttpEntity<>(request, authHeaders()), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void creatingOrderWithInvalidDataReturnsBadRequest() {
        CreateOrderRequest invalid = new CreateOrderRequest(
                2004L,
                "",
                "SKU-BAD",
                -1,
                new BigDecimal("-5.00"),
                "PENDING");

        ResponseEntity<Map> response = restTemplate.exchange(
                "/api/orders", HttpMethod.POST, new HttpEntity<>(invalid, authHeaders()), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}