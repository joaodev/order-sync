package com.joaodev.ordersync.domain;

import java.math.BigDecimal;

public record OrderData(
        Long legacyOrderId,
        String customerName,
        String productCode,
        Integer quantity,
        BigDecimal unitPrice,
        String status
) {
}
