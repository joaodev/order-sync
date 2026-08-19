package com.joaodev.ordersync.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "audit_trail")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuditTrail {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(nullable = false)
    private String action;

    @Column(nullable = false)
    private String source;

    @Column(name = "performed_at", nullable = false)
    private LocalDateTime performedAt;

    @Lob
    private String details;
}
