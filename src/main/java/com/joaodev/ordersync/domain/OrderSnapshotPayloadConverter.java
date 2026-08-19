package com.joaodev.ordersync.domain;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import tools.jackson.databind.json.JsonMapper;

@Converter
public class OrderSnapshotPayloadConverter implements AttributeConverter<OrderSnapshotPayload, String> {

    private static final JsonMapper MAPPER = JsonMapper.builder().findAndAddModules().build();

    @Override
    public String convertToDatabaseColumn(OrderSnapshotPayload attribute) {
        return attribute == null ? null : MAPPER.writeValueAsString(attribute);
    }

    @Override
    public OrderSnapshotPayload convertToEntityAttribute(String dbData) {
        return dbData == null ? null : MAPPER.readValue(dbData, OrderSnapshotPayload.class);
    }
}
