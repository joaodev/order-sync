package com.joaodev.ordersync.service;

public class OrderNotFoundException extends RuntimeException {
    public OrderNotFoundException(Long legacyOrderId) {
        super("No order found with legacyOrderId: " + legacyOrderId);
    }
}
