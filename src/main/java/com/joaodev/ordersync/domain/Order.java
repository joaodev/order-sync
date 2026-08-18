package com.joaodev.ordersync.domain;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "legacy_order_id", nullable = false, unique = true)
    private Long legacyOrderId;

    @Column(name = "customer_name", nullable = false)
    private String customerName;

    @Column(name = "product_code", nullable = false)
    private String productCode;

    @Column(nullable = false)
    private Integer quantity;

    @Column(name = "unit_price", nullable = false)
    private BigDecimal unitPrice;

    @Column(nullable = false)
    private String status;

    @Column(name = "current_version", nullable = false)
    private Integer currentVersion;

    @Column(name = "synced_at", nullable = false)
    private LocalDateTime syncedAt;
}
