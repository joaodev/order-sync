package com.joaodev.ordersync.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "order_snapshots")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderSnapshot {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(nullable = false)
    private Integer version;

    @Lob
    @Convert(converter = OrderSnapshotPayloadConverter.class)
    @Column(nullable = false)
    private OrderSnapshotPayload payload;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
}
