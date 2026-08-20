package com.joaodev.ordersync.kafka;

import com.joaodev.ordersync.domain.OrderData;
import com.joaodev.ordersync.repository.OrderRepository;
import com.joaodev.ordersync.service.OrderVersioningService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;

@Slf4j
@Component
public class OrderCdcListener {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private final OrderRepository orderRepository;
    private final OrderVersioningService orderVersioningService;
    private final IdempotencyService idempotencyService;

    public OrderCdcListener(OrderRepository orderRepository,
                            OrderVersioningService orderVersioningService,
                            IdempotencyService idempotencyService) {
        this.orderRepository = orderRepository;
        this.orderVersioningService = orderVersioningService;
        this.idempotencyService = idempotencyService;
    }

    @KafkaListener(topics = "ordersync.ORDER_SYNC.LEGACY_ORDERS", groupId = "order-sync-consumer")
    public void onMessage(ConsumerRecord<String, String> record) {
        String eventKey = record.topic() + "-" + record.partition() + "-" + record.offset();

        if (!idempotencyService.isNewEvent(eventKey)) {
            log.info("Skipping already-processed CDC event: {}", eventKey);
            return;
        }

        try {
            processEvent(record.value(), eventKey);
        } catch (Exception e) {
            log.error("Failed to process CDC event: {}", eventKey, e.getMessage());
        }
    }

    private void processEvent(String rawValue, String eventKey) {
        JsonNode root = MAPPER.readTree(rawValue);
        JsonNode payload = root.path("payload");
        String op = payload.path("op").asString();

        if ("d".equals(op)) {
            log.info("Ignoring delete event for now: {}", eventKey);
            return;
        }

        JsonNode after = payload.path("after");
        if (after.isMissingNode() || after.isNull()) {
            log.warn("CDC event has no 'after' data, skipping: {}", eventKey);
            return;
        }

        OrderData data = toOrderData(after);
        boolean exists = orderRepository.findByLegacyOrderId(data.legacyOrderId()).isPresent();

        if (exists) {
            orderVersioningService.updateOrder(data.legacyOrderId(), data, "CDC");
        } else {
            orderVersioningService.createOrder(data, "CDC");
        }
    }

    private OrderData toOrderData(JsonNode after) {
        Long legacyOrderId = OracleNumericDecoder.decodeVariableScaleDecimalAsLong(
                after.path("ORDER_ID").path("value").asString());

        String customerName = after.path("CUSTOMER_NAME").asString();
        String productCode = after.path("PRODUCT_CODE").asString();

        Integer quantity = OracleNumericDecoder.decodeVariableScaleDecimalAsInt(
                after.path("QUANTITY").path("value").asString());

        BigDecimal unitPrice = OracleNumericDecoder.decodeDecimal(
                after.path("UNIT_PRICE").asString(), 2);

        String status = after.path("STATUS").asString();

        return new OrderData(legacyOrderId, customerName, productCode, quantity, unitPrice, status);
    }
}
