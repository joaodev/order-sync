package com.joaodev.ordersync.reporting;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

@Repository
public class OrderHistoryRepository {

    private final DSLContext dsl;

    public OrderHistoryRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public List<HistoryEntry> findHistory(Long orderId) {
        List<HistoryEntry>  entries = new ArrayList<>();

        var orderIdField = field(name("order_id"), Long.class);

        dsl.select(
                    field(name("version"), Integer.class),
                    field(name("created_at"), LocalDateTime.class)
                )
                .from(table(name("order_snapshots")))
                .where(orderIdField.eq(orderId))
                .fetch()
                .forEach(r -> entries.add(
                        new HistoryEntry("SNAPSHOT", "version" + r.value1(), r.value2())));

        dsl.select(
                    field(name("from_version"), Integer.class),
                    field(name("to_version"), Integer.class),
                    field(name("created_at"), LocalDateTime.class)
                )
                .from(table(name("order_deltas")))
                .where(orderIdField.eq(orderId))
                .fetch()
                .forEach(r -> entries.add(
                        new HistoryEntry("DELTA", r.value1() + " -> " + r.value2(), r.value3())));

        dsl.select(
                    field(name("action"), String.class),
                    field(name("source"), String.class),
                    field(name("performed_at"), LocalDateTime.class)
                )
                .from(table(name("audit_trail")))
                .where(orderIdField.eq(orderId))
                .fetch()
                .forEach(r -> entries.add(
                    new HistoryEntry(r.value1(), "via " + r.value2(), r.value3())));

        entries.sort(Comparator.comparing(HistoryEntry::occurredAt));
        return entries;
    }
}
