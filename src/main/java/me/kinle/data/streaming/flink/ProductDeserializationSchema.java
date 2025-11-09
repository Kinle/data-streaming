package me.kinle.data.streaming.flink;

import me.kinle.data.streaming.domain.Product;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

public class ProductDeserializationSchema implements DeserializationSchema<Product> {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public Product deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Product.class);
    }

    @Override
    public boolean isEndOfStream(Product nextElement) {
        return null == nextElement;
    }

    @Override
    public TypeInformation<Product> getProducedType() {
        return TypeInformation.of(Product.class);
    }
}
