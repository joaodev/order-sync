package com.joaodev.ordersync.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "order_deltas")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderDelta {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "from_version", nullable = false)
    private Integer fromVersion;

    @Column(name = "to_version", nullable = false)
    private Integer toVersion;

    @Lob
    @Convert(converter = OrderDeltaChangesConverter.class)
    @Column(name = "changed_fields", nullable = false)
    private List<FieldChange> changedFields;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
}
