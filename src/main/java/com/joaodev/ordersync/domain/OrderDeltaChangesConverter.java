package com.joaodev.ordersync.domain;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

@Converter
public class OrderDeltaChangesConverter implements AttributeConverter<List<FieldChange>, String> {

    private static final JsonMapper MAPPER = JsonMapper.builder().findAndAddModules().build();

    @Override
    public String convertToDatabaseColumn(List<FieldChange> attribute) {
        return attribute == null ? null : MAPPER.writeValueAsString(attribute);
    }

    @Override
    public List<FieldChange> convertToEntityAttribute(String dbData) {
        if (dbData == null) return null;
        return List.of(MAPPER.readValue(dbData, FieldChange[].class));
    }
}
