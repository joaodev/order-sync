package com.joaodev.ordersync.reporting;

import com.joaodev.ordersync.domain.Order;
import com.joaodev.ordersync.domain.OrderData;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
public class ReportingIT {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart");

    @DynamicPropertySource
    static void registerDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", oracle::getJdbcUrl);
        registry.add("spring.datasource.username", oracle::getUsername);
        registry.add("spring.datasource.password", oracle::getPassword);
    }

    @Autowired
    private OrderVersioningService versioningService;

    @Autowired
    private OrderHistoryRepository historyRepository;

    @Autowired
    private OrderReportRepository reportRepository;

    @Test
    void historyIncludesSnapshotsDeltasAndAuditInChronologicalOrder() {
        OrderData original = new OrderData(
                6001L,
                "History Test Corp",
                "SKU-HIST",
                2,
                new BigDecimal("40.00"),
                "PENDING");

        Order created = versioningService.createOrder(original, "TEST");

        OrderData changed = new OrderData(
                6001L,
                "History Test Corp",
                "SKU-HIST",
                2,
                new BigDecimal("40.00"),
                "CONFIRMED");

        versioningService.updateOrder(6001L, changed, "TEST");

        List<HistoryEntry> history = historyRepository.findHistory(created.getId());

        assertThat(history).hasSize(5);
        assertThat(history.getFirst().eventType()).isEqualTo("SNAPSHOT");
        assertThat(history.getFirst().detail()).isEqualTo("version 1");

        assertThat(history)
                .extracting(HistoryEntry::eventType)
                .contains("SNAPSHOT", "CREATED", "DELTA", "UPDATED");

        assertThat(history).isSortedAccordingTo(
                (a, b) -> a.occurredAt().compareTo(b.occurredAt()));
    }

    @Test
    void summaryGroupsOrdersByStatusWithCorrectTotals() {
        versioningService.createOrder(new OrderData(
                6002L,
                "Report Test A",
                "SKU-A",
                2,
                new BigDecimal("10.00"),
                "PENDING"), "TEST");

        versioningService.createOrder(new OrderData(
                6003L,
                "Report Test B",
                "SKU-B",
                3,
                new BigDecimal("20.00"),
                "PENDING"), "TEST");

        versioningService.createOrder(new OrderData(
                6004L,
                "Report Test C",
                "SKU-C",
                1,
                new BigDecimal("100.00"),
                "CONFIRMED"), "TEST");

        List<StatusSummary> summary = reportRepository.summarizeByStatus();

        StatusSummary pending = summary.stream()
                .filter(s -> s.status().equals("PENDING"))
                .findFirst().orElseThrow();

        assertThat(pending.orderCount()).isEqualTo(2);
        assertThat(pending.totalValue()).isEqualByComparingTo("80.00");

        StatusSummary confirmed = summary.stream()
                .filter(s -> s.status().equals("CONFIRMED"))
                .findFirst().orElseThrow();

        assertThat(confirmed.orderCount()).isEqualTo(1);
        assertThat(confirmed.totalValue()).isEqualByComparingTo("100.00");
    }
}
