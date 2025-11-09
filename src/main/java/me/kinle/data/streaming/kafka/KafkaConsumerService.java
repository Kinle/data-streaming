package me.kinle.data.streaming.kafka;

import com.google.common.base.Stopwatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.kinle.data.streaming.ProductRepository;
import me.kinle.data.streaming.domain.Product;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaConsumerService {

    public static final String TEST_TOPIC = "test-topic";

    public final ProductRepository productRepository;

    private Stopwatch stopwatch = Stopwatch.createUnstarted();



//    @KafkaListener(topics = TEST_TOPIC, groupId = "kafka-listener-1")
    public void consume(Product product) {
        if(!stopwatch.isRunning()){
            stopwatch.start();
        }
        productRepository.save(product); // savg
        if(product.getId() % 1_000_000 == 0 || product.getId() == 9999999){
            log.info("Consumed: {} in {}", product.getId(), stopwatch.elapsed());
        }

    }

}
