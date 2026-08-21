package com.joaodev.ordersync.batch;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderCsvRecord {
    private Long legacyOrderId;
    private String customerName;
    private String productCode;
    private Integer quantity;
    private BigDecimal unitPrice;
    private String status;
}
