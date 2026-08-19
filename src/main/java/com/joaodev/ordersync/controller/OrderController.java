package com.joaodev.ordersync.controller;

import com.joaodev.ordersync.controller.dto.CreateOrderRequest;
import com.joaodev.ordersync.controller.dto.UpdateOrderRequest;
import com.joaodev.ordersync.domain.Order;
import com.joaodev.ordersync.domain.OrderData;
import com.joaodev.ordersync.repository.OrderRepository;
import com.joaodev.ordersync.service.OrderVersioningService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderRepository orderRepository;
    private final OrderVersioningService versioningService;

    public OrderController(OrderRepository orderRepository, OrderVersioningService versioningService) {
        this.orderRepository = orderRepository;
        this.versioningService = versioningService;
    }

    @GetMapping
    public List<Order> listOrders() {
        return orderRepository.findAll();
    }

    @PostMapping
    public ResponseEntity<Order> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        OrderData data = new OrderData(
                request.legacyOrderId(), request.customerName(), request.productCode(),
                request.quantity(), request.unitPrice(), request.status()
        );

        Order created = versioningService.createOrder(data, "MINIMAL");

        return ResponseEntity
                .created(URI.create("/api/orders/" + created.getLegacyOrderId()))
                .body(created);
    }

    @PutMapping("/{legacyOrderId}")
    public ResponseEntity<Order> updateOrder(@PathVariable Long legacyOrderId,
                                             @Valid @RequestBody UpdateOrderRequest request) {
        OrderData data = new OrderData(
                legacyOrderId, request.customerName(), request.productCode(),
                request.quantity(), request.unitPrice(), request.status()
        );
        Order updated = versioningService.updateOrder(legacyOrderId, data, "MINIMAL");
        return ResponseEntity.ok(updated);
    }
}
