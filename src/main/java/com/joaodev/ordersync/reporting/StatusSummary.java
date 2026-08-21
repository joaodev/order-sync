package com.joaodev.ordersync.reporting;

import java.math.BigDecimal;

public record StatusSummary(String status, Integer orderCount, BigDecimal totalValue) {
}
