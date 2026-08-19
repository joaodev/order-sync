package com.joaodev.ordersync.repository;

import com.joaodev.ordersync.domain.OrderSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderSnapshotRepository extends JpaRepository<OrderSnapshot, Long> {
}
