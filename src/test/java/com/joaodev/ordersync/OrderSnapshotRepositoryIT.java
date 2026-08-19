package com.joaodev.ordersync;

import com.joaodev.ordersync.domain.Order;
import com.joaodev.ordersync.domain.OrderSnapshot;
import com.joaodev.ordersync.domain.OrderSnapshotPayload;
import com.joaodev.ordersync.repository.OrderRepository;
import com.joaodev.ordersync.repository.OrderSnapshotRepository;
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
public class OrderSnapshotRepositoryIT {

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
    private OrderSnapshotRepository orderSnapshotRepository;

    @Test
    void saveAndReconstructsSnapshotPayload() {
        Order order = orderRepository.save(Order.builder()
                .legacyOrderId(999L)
                .customerName("Test Customer")
                .productCode("SKU-TEST")
                .quantity(2)
                .unitPrice(new BigDecimal("25.00"))
                .status("PENDING")
                .currentVersion(1)
                .syncedAt(LocalDateTime.now())
                .build()
        );

        OrderSnapshotPayload payload = new OrderSnapshotPayload(
                order.getLegacyOrderId(),
                order.getCustomerName(),
                order.getProductCode(),
                order.getQuantity(),
                order.getUnitPrice(),
                order.getStatus(),
                order.getCurrentVersion(),
                order.getSyncedAt()
        );

        OrderSnapshot snapshot = OrderSnapshot.builder()
                .orderId(order.getId())
                .version(1)
                .payload(payload)
                .createdAt(LocalDateTime.now())
                .build();

        OrderSnapshot saved = orderSnapshotRepository.save(snapshot);
        orderSnapshotRepository.flush();

        OrderSnapshot reloaded = orderSnapshotRepository.findById(saved.getId()).orElseThrow();

        assertThat(reloaded.getPayload().customerName()).isEqualTo("Test Customer");
        assertThat(reloaded.getPayload().unitPrice()).isEqualTo(new BigDecimal("25.00"));
        assertThat(reloaded.getPayload().quantity()).isEqualTo(2);
        assertThat(reloaded.getPayload().productCode()).isEqualTo("SKU-TEST");
        assertThat(reloaded.getPayload().status()).isEqualTo("PENDING");
        assertThat(reloaded.getPayload().legacyOrderId()).isEqualTo(999L);
    }
}
