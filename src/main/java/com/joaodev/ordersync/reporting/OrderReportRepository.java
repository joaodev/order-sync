package com.joaodev.ordersync.reporting;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

import static org.jooq.impl.DSL.*;

@Repository
public class OrderReportRepository {

    private final DSLContext dsl;

    public OrderReportRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public List<StatusSummary> summarizeByStatus() {
        var status = field(name("status"), String.class);
        var quantity = field(name("quantity"), Integer.class);
        var unitPrice = field(name("unit_price"), BigDecimal.class);

        return dsl.select(status, count(), sum(quantity.cast(BigDecimal.class).mul(unitPrice)))
                .from(table(name("orders")))
                .groupBy(status)
                .orderBy(status)
                .fetch()
                .map(r -> new StatusSummary(r.value1(), r.value2(), r.value3()));
    }
}
