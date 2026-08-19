package com.joaodev.ordersync;

import com.joaodev.ordersync.domain.FieldChange;
import com.joaodev.ordersync.domain.Order;
import com.joaodev.ordersync.domain.OrderDelta;
import com.joaodev.ordersync.repository.OrderDeltaRepository;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
public class OrderDeltaRepositoryIT {

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
    private OrderDeltaRepository orderDeltaRepository;

    @Test
    void savesAndReconstructsChangedFields() {
        Order order = orderRepository.save(Order.builder()
                .legacyOrderId(998L)
                .customerName("Delta Test Customer")
                .productCode("SKU-DELTA")
                .quantity(1)
                .unitPrice(new BigDecimal("15.00"))
                .status("CONFIRMED")
                .currentVersion(2)
                .syncedAt(LocalDateTime.now())
                .build());

        List<FieldChange> changes = List.of(
                new FieldChange("status", "PENDING", "CONFIRMED")
        );

        OrderDelta delta = OrderDelta.builder()
                .orderId(order.getId())
                .fromVersion(1)
                .toVersion(2)
                .changedFields(changes)
                .createdAt(LocalDateTime.now())
                .build();

        OrderDelta saved = orderDeltaRepository.save(delta);
        orderDeltaRepository.flush();

        OrderDelta reloaded = orderDeltaRepository.findById(saved.getId()).orElseThrow();

        assertThat(reloaded.getChangedFields()).hasSize(1);
        assertThat(reloaded.getChangedFields().getFirst().fieldName()).isEqualTo("status");
        assertThat(reloaded.getChangedFields().getFirst().newValue()).isEqualTo("CONFIRMED");
        assertThat(reloaded.getChangedFields().getFirst().oldValue()).isEqualTo("PENDING");
        assertThat(reloaded.getOrderId()).isEqualTo(order.getId());
        assertThat(reloaded.getFromVersion()).isEqualTo(1);
        assertThat(reloaded.getToVersion()).isEqualTo(2);
    }
}
