package com.joaodev.ordersync.controller.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotNull Long legacyOrderId,
        @NotBlank String customerName,
        @NotBlank String productCode,
        @NotNull @Positive Integer quantity,
        @NotNull @Positive BigDecimal unitPrice,
        @NotBlank String status
) {
}
