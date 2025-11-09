# Data Streaming Example Project

### Setup
- Create .env file from .env example. Modify if needed
- Run the `docker-compose up -d` to start the kafka and postgres containers
- Run first time load of 10M records by hitting the [`products/load`](http://localhost:8080/products/load) API
- Uncomment [Kafka Listener](src/main/java/me/kinle/data/streaming/kafka/KafkaConsumerService.java) when needed
- JVM Args to be added to run configuration
  - --add-opens java.base/java.util=ALL-UNNAMED (Java 17 or above)
  -  -Xmx1G (For maximum heap memory)



