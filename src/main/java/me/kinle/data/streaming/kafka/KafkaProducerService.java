package me.kinle.data.streaming.kafka;

import me.kinle.data.streaming.domain.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    private static final String TOPIC = "test-topic";

    @Autowired
    private KafkaTemplate<String, Product> kafkaTemplate;

    public void sendProduct(Product product) {
        kafkaTemplate.send(TOPIC, product);
    }
}
