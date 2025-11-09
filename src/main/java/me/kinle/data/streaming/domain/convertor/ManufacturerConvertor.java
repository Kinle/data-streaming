package me.kinle.data.streaming.domain.convertor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.kinle.data.streaming.domain.Manufacturer;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Converter
public class ManufacturerConvertor implements AttributeConverter<Manufacturer,String> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(Manufacturer object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException jpe) {
            log.warn("Cannot convert into JSON");
            return null;
        }
    }

    @Override
    public Manufacturer convertToEntityAttribute(String value) {
        try {
            return objectMapper.readValue(value, Manufacturer.class);
        } catch (JsonProcessingException e) {
            log.warn("Cannot convert JSON into Value");
            return null;
        }
    }
}
