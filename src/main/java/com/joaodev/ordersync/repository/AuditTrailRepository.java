package com.joaodev.ordersync.repository;

import com.joaodev.ordersync.domain.AuditTrail;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AuditTrailRepository extends JpaRepository<AuditTrail, Long> {
}
