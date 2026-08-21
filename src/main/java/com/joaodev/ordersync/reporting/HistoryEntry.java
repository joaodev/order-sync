package com.joaodev.ordersync.reporting;

import java.time.LocalDateTime;

public record HistoryEntry(String eventType, String detail, LocalDateTime occurredAt) {
}
