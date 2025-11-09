package me.kinle.data.streaming.flink;


import com.google.common.base.Stopwatch;
import me.kinle.data.streaming.domain.convertor.DimensionsConvertor;
import me.kinle.data.streaming.domain.convertor.ManufacturerConvertor;
import me.kinle.data.streaming.domain.convertor.PriceInfoConvertor;
import me.kinle.data.streaming.domain.convertor.ProductDetailsConvertor;
import me.kinle.data.streaming.domain.Product;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.jetbrains.annotations.Nullable;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.time.Duration;

@Slf4j
@Service
@AllArgsConstructor
public class FlinkKafkaService {

    private static final PriceInfoConvertor priceInfoConvertor = new PriceInfoConvertor();
    private static final ManufacturerConvertor manufacturerConvertor = new ManufacturerConvertor();
    private static final ProductDetailsConvertor productDetailsConvertor = new ProductDetailsConvertor();
    private static final DimensionsConvertor dimensionsConvertor = new DimensionsConvertor();
    private final KafkaProperties kafkaProperties;
    private final DataSourceProperties dataSourceProperties;

    private static JdbcSinkBuilder<Product> addQueryStatement(JdbcSinkBuilder<Product> builder) {
        return builder
                .withQueryStatement(
                        "INSERT INTO product (id, name, description, category, tags, created_date, updated_date, price_info, manufacturer, details, available, stock, weight, dimensions, ratings, images, barcode, sku, expiry_date, discount) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (ps, product) -> {
                            ps.setLong(1, product.getId());
                            ps.setString(2, product.getName());
                            ps.setString(3, product.getDescription());
                            ps.setString(4, product.getCategory());
                            ps.setArray(5, product.getTags() != null ? ps.getConnection().createArrayOf("VARCHAR", product.getTags().toArray(String[]::new)) : null);
                            ps.setDate(6, product.getCreatedDate() != null ? Date.valueOf(product.getCreatedDate()) : null);
                            ps.setDate(7, product.getUpdatedDate() != null ? Date.valueOf(product.getUpdatedDate()) : null);
                            ps.setString(8, product.getPriceInfo() != null ? priceInfoConvertor.convertToDatabaseColumn(product.getPriceInfo()) : null);
                            ps.setString(9, product.getManufacturer() != null ? manufacturerConvertor.convertToDatabaseColumn(product.getManufacturer()) : null);
                            ps.setString(10, product.getDetails() != null ? productDetailsConvertor.convertToDatabaseColumn(product.getDetails()) : null);
                            ps.setBoolean(11, product.isAvailable());
                            ps.setInt(12, product.getStock());
                            ps.setDouble(13, product.getWeight());
                            ps.setString(14, product.getDimensions() != null ? dimensionsConvertor.convertToDatabaseColumn(product.getDimensions()) : null);
                            ps.setArray(15, product.getRatings() != null ? ps.getConnection().createArrayOf("double", product.getRatings().toArray(Double[]::new)) : null);
                            ps.setArray(16, product.getImages() != null ? ps.getConnection().createArrayOf("varchar", product.getImages().toArray(String[]::new)) : null);
                            ps.setString(17, product.getBarcode());
                            ps.setString(18, product.getSku());
                            ps.setDate(19, product.getExpiryDate() != null ? Date.valueOf(product.getExpiryDate()) : null);
                            ps.setDouble(20, product.getDiscount());
                        }
                );
    }

    public <T> KafkaSource<T> createConsumerForTopic(String kafkaGroup, DeserializationSchema<T> deserializationSchema) {
        return KafkaSource
                .<T>builder()
                .setBootstrapServers(StringUtils.join(kafkaProperties.getBootstrapServers(), ","))
                .setTopics("test-topic")
                .setGroupId(kafkaGroup)
                .setValueOnlyDeserializer(deserializationSchema)
                .setBounded(OffsetsInitializer.latest())
                .build();
    }

    public void processToFile() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment();
        environment.setParallelism(Runtime.getRuntime().availableProcessors());
        environment.enableCheckpointing(Duration.ofMinutes(1).toMillis());
        KafkaSource<String> flinkKafkaConsumer = createConsumerForTopic("flink-1", new SimpleStringSchema());
        DataStreamSource<String> inputStream = environment.fromSource(flinkKafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka");
        sinkToFile(inputStream);
        execute(environment, stopwatch);
    }

    private static void execute(StreamExecutionEnvironment environment, Stopwatch stopwatch) throws Exception {
        environment.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
                log.info("The process started after {} min", stopwatch.elapsed().toMinutesPart());
            }

            @Override
            public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
                log.info("The process took {}", stopwatch.elapsed());
            }
        });
        environment.execute("Flink Kafka Producer");
        log.info("The process took {} min", stopwatch.elapsed().toMinutesPart());
    }

    private static void sinkToFile(DataStreamSource<String> inputStream) {
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(new Path("./flink/json"), new SimpleStringEncoder<>())
                .withOutputFileConfig(OutputFileConfig.builder().withPartSuffix(".json").build())
                .build();
        inputStream.sinkTo(fileSink);
    }

    public void processIntoDb() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment();
        environment.setParallelism(Runtime.getRuntime().availableProcessors());
        environment.enableCheckpointing(Duration.ofMinutes(1).toMillis());
        KafkaSource<Product> flinkKafkaConsumer = createConsumerForTopic("flink-1", new ProductDeserializationSchema());
        DataStreamSource<Product> inputStream = environment.fromSource(flinkKafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka");
        sinkToJdbc(inputStream);
        execute(environment, stopwatch);
    }

    public void processAsNone() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment();
        environment.setParallelism(Runtime.getRuntime().availableProcessors());
        environment.enableCheckpointing(Duration.ofMinutes(1).toMillis());
        KafkaSource<Product> flinkKafkaConsumer = createConsumerForTopic("flink-1", new ProductDeserializationSchema());
        DataStreamSource<Product> inputStream = environment.fromSource(flinkKafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka");
        inputStream.sinkTo(
                new DiscardingSink<>()
        );
        execute(environment, stopwatch);
    }

    private void sinkToJdbc(DataStreamSource<Product> inputStream) {
        var options = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(dataSourceProperties.determineDriverClassName())
                .withUsername(dataSourceProperties.determineUsername())
                .withPassword(dataSourceProperties.determinePassword())
                .withUrl(dataSourceProperties.determineUrl())
                .build();
        JdbcSinkBuilder<Product> builder = JdbcSink.builder();
        var jdbcSink = addQueryStatement(builder)
                .buildAtLeastOnce(options);
        inputStream.sinkTo(jdbcSink);
    }
}
