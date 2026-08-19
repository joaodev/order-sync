package com.joaodev.ordersync;

import com.joaodev.ordersync.domain.AuditTrail;
import com.joaodev.ordersync.domain.Order;
import com.joaodev.ordersync.repository.AuditTrailRepository;
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
public class AuditTrailRepositoryIT {

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

    @Autowired
    private AuditTrailRepository auditTrailRepository;

    @Test
    void savesAndRetrievesAuditEntry() {
        Order order = orderRepository.save(Order.builder()
                .legacyOrderId(997L)
                .customerName("Audit Test Customer")
                .productCode("SKU-AUDIT")
                .quantity(1)
                .unitPrice(new BigDecimal("10.00"))
                .status("PENDING")
                .currentVersion(1)
                .syncedAt(LocalDateTime.now())
                .build());

        AuditTrail entry = AuditTrail.builder()
                .orderId(order.getId())
                .action("CREATED")
                .source("CDC")
                .performedAt(LocalDateTime.now())
                .details("Order created from legacy CDC event")
                .build();

        AuditTrail saved = auditTrailRepository.save(entry);

        assertThat(saved.getId()).isNotNull();
        assertThat(auditTrailRepository.findById(saved.getId()))
                .isPresent()
                .get()
                .extracting(AuditTrail::getAction)
                .isEqualTo("CREATED");
    }
}
