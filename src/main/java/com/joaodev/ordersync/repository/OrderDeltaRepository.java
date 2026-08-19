package com.joaodev.ordersync.repository;

import com.joaodev.ordersync.domain.OrderDelta;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderDeltaRepository extends JpaRepository<OrderDelta, Long> {
}
