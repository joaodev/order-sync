package com.joaodev.ordersync.service;

public class OrderAlreadyExistsException extends RuntimeException {
    public OrderAlreadyExistsException(Long legacyOrderId) {
        super("Order already exists with legacyOrderId: " + legacyOrderId);
    }
}
