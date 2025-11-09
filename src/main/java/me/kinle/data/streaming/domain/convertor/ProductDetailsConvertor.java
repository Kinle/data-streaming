package me.kinle.data.streaming.domain.convertor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.kinle.data.streaming.domain.ProductDetails;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Converter
public class ProductDetailsConvertor implements AttributeConverter<ProductDetails,String> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(ProductDetails object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException jpe) {
            log.warn("Cannot convert into JSON");
            return null;
        }
    }

    @Override
    public ProductDetails convertToEntityAttribute(String value) {
        try {
            return objectMapper.readValue(value, ProductDetails.class);
        } catch (JsonProcessingException e) {
            log.warn("Cannot convert JSON into Value");
            return null;
        }
    }
}
