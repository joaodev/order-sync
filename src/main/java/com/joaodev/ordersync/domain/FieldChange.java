package com.joaodev.ordersync.domain;

public record FieldChange(String fieldName, String oldValue, String newValue) {
}
