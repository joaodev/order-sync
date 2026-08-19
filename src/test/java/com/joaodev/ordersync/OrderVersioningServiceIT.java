package com.joaodev.ordersync;

import com.joaodev.ordersync.domain.Order;
import com.joaodev.ordersync.domain.OrderData;
import com.joaodev.ordersync.repository.AuditTrailRepository;
import com.joaodev.ordersync.repository.OrderDeltaRepository;
import com.joaodev.ordersync.repository.OrderRepository;
import com.joaodev.ordersync.repository.OrderSnapshotRepository;
import com.joaodev.ordersync.service.OrderAlreadyExistsException;
import com.joaodev.ordersync.service.OrderNotFoundException;
import com.joaodev.ordersync.service.OrderVersioningService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Testcontainers
public class OrderVersioningServiceIT {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart");

    @DynamicPropertySource
    static void registerDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", oracle::getJdbcUrl);
        registry.add("spring.datasource.username", oracle::getUsername);
        registry.add("spring.datasource.password", oracle::getPassword);
    }

    @Autowired
    private OrderVersioningService versioningService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderSnapshotRepository orderSnapshotRepository;

    @Autowired
    private OrderDeltaRepository orderDeltaRepository;

    @Autowired
    private AuditTrailRepository auditTrailRepository;

    @Test
    void createsOrderWithInitialSnapshotAndAuditEntry() {
        OrderData data = new OrderData(1001L, "Acme Corp", "SKU-001",
                5, new BigDecimal("10.00"), "PENDING");

        Order created = versioningService.createOrder(data, "CDC");

        assertThat(created.getId()).isNotNull();
        assertThat(created.getCurrentVersion()).isEqualTo(1);

        assertThat(orderSnapshotRepository.findAll())
                .filteredOn(s -> s.getOrderId().equals(created.getId()))
                .hasSize(1)
                .first()
                .satisfies(s -> assertThat(s.getVersion()).isEqualTo(1));

        assertThat(orderDeltaRepository.findAll())
                .filteredOn(d -> d.getOrderId().equals(created.getId()))
                .isEmpty();

        assertThat(auditTrailRepository.findAll())
                .filteredOn(a -> a.getOrderId().equals(created.getId()))
                .hasSize(1)
                .first()
                .satisfies(a -> assertThat(a.getAction()).isEqualTo("CREATED"));
    }

    @Test
    void updatingOrderCreatesNewSnapshotDeltaAndAuditEntry() {
        OrderData original = new OrderData(1002L, "Globex Ltd", "SKU-002", 3,
                new BigDecimal("50.00"), "PENDING");
        Order created = versioningService.createOrder(original, "CDC");

        OrderData changed = new OrderData(1002L, "Globex Ltd", "SKU-002", 3,
                new BigDecimal("50.00"), "CONFIRMED");
        Order updated = versioningService.updateOrder(1002L, changed, "CDC");

        assertThat(updated.getCurrentVersion()).isEqualTo(2);
        assertThat(updated.getStatus()).isEqualTo("CONFIRMED");

        assertThat(orderSnapshotRepository.findAll())
                .filteredOn(s -> s.getOrderId().equals(created.getId()))
                .hasSize(2);

        assertThat(orderDeltaRepository.findAll())
                .filteredOn(d -> d.getOrderId().equals(created.getId()))
                .hasSize(1)
                .first()
                .satisfies(d -> {
                    assertThat(d.getFromVersion()).isEqualTo(1);
                    assertThat(d.getToVersion()).isEqualTo(2);
                    assertThat(d.getChangedFields())
                            .hasSize(1)
                            .first()
                            .satisfies(change -> {
                                assertThat(change.fieldName()).isEqualTo("status");
                                assertThat(change.oldValue()).isEqualTo("PENDING");
                                assertThat(change.newValue()).isEqualTo("CONFIRMED");
                            });
                });

        assertThat(auditTrailRepository.findAll())
                .filteredOn(a -> a.getOrderId().equals(created.getId()))
                .hasSize(2)
                .extracting("action")
                .containsExactlyInAnyOrder("CREATED", "UPDATED");
    }

    @Test
    void updatingWithNoActualChangesDoesNotCreateNewVersion() {
        OrderData original = new OrderData(1003L, "Initech", "SKU-003", 1,
                new BigDecimal("99.90"), "PENDING");
        Order created = versioningService.createOrder(original, "CDC");

        Order result = versioningService.updateOrder(1003L, original, "CDC");

        assertThat(result.getCurrentVersion()).isEqualTo(1);
        assertThat(orderSnapshotRepository.findAll())
                .filteredOn(s -> s.getOrderId().equals(created.getId()))
                .hasSize(1);
        assertThat(orderDeltaRepository.findAll())
                .filteredOn(d -> d.getOrderId().equals(created.getId()))
                .isEmpty();
    }

    @Test
    void creatingDuplicateOrderThrows() {
        OrderData data = new OrderData(1004L, "Umbrella Corp", "SKU-004",
                2, new BigDecimal("20.00"), "PENDING");
        versioningService.createOrder(data, "CDC");

        assertThatThrownBy(() -> versioningService.createOrder(data, "CDC"))
                .isInstanceOf(OrderAlreadyExistsException.class);
    }

    @Test
    void updatingNonExistentOrderThrows() {
        OrderData data = new OrderData(
                999L,
                "Ghost Corp",
                "SKU-999",
                1,
                new BigDecimal("1.00"),
                "PENDING"
        );

        assertThatThrownBy(() -> versioningService.updateOrder(9999L, data, "CDC"))
                .isInstanceOf(OrderNotFoundException.class);
    }
}
