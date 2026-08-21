package com.joaodev.ordersync.reporting;

import com.joaodev.ordersync.repository.OrderRepository;
import com.joaodev.ordersync.service.OrderNotFoundException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class ReportController {

    private final OrderRepository orderRepository;
    private final OrderHistoryRepository historyRepository;
    private final OrderReportRepository reportRepository;

    public ReportController(OrderRepository orderRepository,
                            OrderHistoryRepository historyRepository,
                            OrderReportRepository reportRepository) {
        this.orderRepository = orderRepository;
        this.historyRepository = historyRepository;
        this.reportRepository = reportRepository;
    }

    @GetMapping("/orders/{legacyOrderId}/history")
    public List<HistoryEntry> getOrderHistory(@PathVariable Long legacyOrderId) {
        var order = orderRepository.findByLegacyOrderId(legacyOrderId)
                .orElseThrow(() -> new OrderNotFoundException(legacyOrderId));
        return historyRepository.findHistory(order.getId());
    }

    @GetMapping("/reports/orders-summary")
    public List<StatusSummary> getOrdersSummary() {
        return reportRepository.summarizeByStatus();
    }
}
