package com.joaodev.ordersync;

import com.joaodev.ordersync.domain.Order;
import com.joaodev.ordersync.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
public class OrderRepositoryIT {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart");

    @DynamicPropertySource
    static void registerDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", oracle::getJdbcUrl);
        registry.add("spring.datasource.username", oracle::getUsername);
        registry.add("spring.datasource.password", oracle::getPassword);
    }

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void saveAsRetrievesAnOrder() {
        Order order = Order.builder()
                .legacyOrderId(999L)
                .customerName("Test Customer")
                .productCode("SKU-TEST")
                .quantity(1)
                .unitPrice(new BigDecimal("10.00"))
                .status("PENDING")
                .currentVersion(1)
                .syncedAt(LocalDateTime.now())
                .build();

        Order saved = orderRepository.save(order);

        assertThat(saved.getId()).isNotNull();
        assertThat(orderRepository.findById(saved.getId())).isPresent();
    }
}
