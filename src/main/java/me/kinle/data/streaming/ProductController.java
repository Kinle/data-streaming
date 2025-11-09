package me.kinle.data.streaming;

import me.kinle.data.streaming.domain.Product;
import me.kinle.data.streaming.domain.ProductGenerator;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.kinle.data.streaming.flink.FlinkKafkaService;
import me.kinle.data.streaming.kafka.KafkaProducerService;
import me.kinle.data.streaming.spark.SparkDeltaKafkaService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.stream.LongStream;

@RestController
@RequestMapping("/products")
@AllArgsConstructor
@Slf4j
public class ProductController {

    private final KafkaProducerService producerService;
    private final SparkDeltaKafkaService sparkDeltaKafkaService;
    private final FlinkKafkaService flinkKafkaService;

    @GetMapping("/load")
    public ResponseEntity<Object> loadProducts() {
        LongStream.range(0, 10_000_000).forEach(i -> {
            try{
                if(i % 100_000 == 0){
                    log.info("Loading product {}", i);
                }
                Product product = ProductGenerator.randomProduct(i);
                producerService.sendProduct(product);
            }catch (Exception e){
                log.error("Error while sending product:{} to kafka",i ,e);
            }
        });
        return ResponseEntity.accepted().build();
    }

    @GetMapping("/spark")
    public ResponseEntity<Object> spark() throws Exception {
        sparkDeltaKafkaService.readKafkaWriteDeltaAndCsv();
        return ResponseEntity.ok().build();
    }

    @GetMapping("/flink/file")
    public ResponseEntity<Object> flinkToFile() throws Exception {
        flinkKafkaService.processToFile();
        return ResponseEntity.ok().build();
    }

    @GetMapping("/flink/db")
    public ResponseEntity<Object> flinkToDb() throws Exception {
        flinkKafkaService.processIntoDb();
        return ResponseEntity.ok().build();
    }

    @GetMapping("/flink")
    public ResponseEntity<Object> flinkPrint() throws Exception {
        flinkKafkaService.processAsNone();
        return ResponseEntity.ok().build();
    }

}


