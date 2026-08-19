package com.joaodev.ordersync.repository;

import com.joaodev.ordersync.domain.Order;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface OrderRepository extends JpaRepository<Order, Long> {
    Optional<Order> findByLegacyOrderId(Long legacyOrderId);
}
