package com.joaodev.ordersync.service;

import com.joaodev.ordersync.domain.*;
import com.joaodev.ordersync.repository.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class OrderVersioningService {

    private final OrderRepository orderRepository;
    private final OrderSnapshotRepository orderSnapshotRepository;
    private final OrderDeltaRepository orderDeltaRepository;
    private final AuditTrailRepository auditTrailRepository;

    public OrderVersioningService(OrderRepository orderRepository,
                                  OrderSnapshotRepository orderSnapshotRepository,
                                  OrderDeltaRepository orderDeltaRepository,
                                  AuditTrailRepository auditTrailRepository) {
        this.orderRepository = orderRepository;
        this.orderSnapshotRepository = orderSnapshotRepository;
        this.orderDeltaRepository = orderDeltaRepository;
        this.auditTrailRepository = auditTrailRepository;
    }

    @Transactional
    public Order createOrder(OrderData data, String source) {
        orderRepository.findByLegacyOrderId(data.legacyOrderId()).ifPresent(existing -> {
            throw new OrderAlreadyExistsException(data.legacyOrderId());
        });

        Order order = Order.builder()
                .legacyOrderId(data.legacyOrderId())
                .customerName(data.customerName())
                .productCode(data.productCode())
                .quantity(data.quantity())
                .unitPrice(data.unitPrice())
                .status(data.status())
                .currentVersion(1)
                .syncedAt(LocalDateTime.now())
                .build();

        Order saved = orderRepository.save(order);

        orderSnapshotRepository.save(OrderSnapshot.builder()
                .orderId(saved.getId())
                .version(1)
                .payload(toPayload(saved))
                .createdAt(LocalDateTime.now())
                .build());

        auditTrailRepository.save(AuditTrail.builder()
                .orderId(saved.getId())
                .action("CREATED")
                .source(source)
                .performedAt(LocalDateTime.now())
                .details("Order created with initial version 1")
                .build());

        return saved;
    }

    @Transactional
    public Order updateOrder(Long legacyOrderId, OrderData data, String source) {
        Order existing = orderRepository.findByLegacyOrderId(legacyOrderId)
                .orElseThrow(() -> new OrderNotFoundException(legacyOrderId));

        List<FieldChange> changes = detectChanges(existing, data);

        if (changes.isEmpty()) return existing;

        int previousVersion = existing.getCurrentVersion();
        int newVersion = previousVersion + 1;

        existing.setCustomerName(data.customerName());
        existing.setProductCode(data.productCode());
        existing.setQuantity(data.quantity());
        existing.setUnitPrice(data.unitPrice());
        existing.setStatus(data.status());
        existing.setCurrentVersion(newVersion);
        existing.setSyncedAt(LocalDateTime.now());

        Order saved = orderRepository.save(existing);

        orderSnapshotRepository.save(OrderSnapshot.builder()
                .orderId(saved.getId())
                .version(newVersion)
                .payload(toPayload(saved))
                .createdAt(LocalDateTime.now())
                .build());

        orderDeltaRepository.save(OrderDelta.builder()
                .orderId(saved.getId())
                .fromVersion(previousVersion)
                .toVersion(newVersion)
                .changedFields(changes)
                .createdAt(LocalDateTime.now())
                .build());

        auditTrailRepository.save(AuditTrail.builder()
                .orderId(saved.getId())
                .action("UPDATED")
                .source(source)
                .performedAt(LocalDateTime.now())
                .details("Order updated from version " + previousVersion + " to " + newVersion)
                .build());

        return saved;
    }

    private List<FieldChange> detectChanges(Order existing, OrderData data) {
        List<FieldChange> changes = new ArrayList<>();

        addIfChanged(changes, "customerName", existing.getCustomerName(), data.customerName());
        addIfChanged(changes, "productCode", existing.getProductCode(), data.productCode());
        addIfChanged(changes, "quantity", existing.getQuantity(), data.quantity());
        addIfChanged(changes, "unitPrice", existing.getUnitPrice(), data.unitPrice());
        addIfChanged(changes, "status", existing.getStatus(), data.status());

        return changes;
    }

    private void addIfChanged(List<FieldChange> changes, String fieldName, Object oldValue, Object newValue) {
        if (!Objects.equals(oldValue, newValue)) {
            changes.add(new FieldChange(fieldName,
                    oldValue == null ? null : oldValue.toString(),
                    newValue == null ? null : newValue.toString()));
        }
    }

    private void addIfChanged(List<FieldChange> changes, String fieldName, BigDecimal oldValue, BigDecimal newValue) {
        boolean changed = (oldValue == null || newValue == null)
                ? !Objects.equals(oldValue, newValue)
                : oldValue.compareTo(newValue) != 0;

        if (changed) {
            changes.add(new FieldChange(fieldName,
                    oldValue == null ? null : oldValue.toString(),
                    newValue == null ? null : newValue.toString()));
        }
    }

    private OrderSnapshotPayload toPayload(Order order) {
        return new OrderSnapshotPayload(
                order.getLegacyOrderId(),
                order.getCustomerName(),
                order.getProductCode(),
                order.getQuantity(),
                order.getUnitPrice(),
                order.getStatus(),
                order.getCurrentVersion(),
                LocalDateTime.now()
        );
    }
}
