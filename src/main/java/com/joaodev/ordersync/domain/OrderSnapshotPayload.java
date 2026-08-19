package com.joaodev.ordersync.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record OrderSnapshotPayload(
        Long legacyOrderId,
        String customerName,
        String productCode,
        Integer quantity,
        BigDecimal unitPrice,
        String status,
        Integer version,
        LocalDateTime capturedAt
) {
}
