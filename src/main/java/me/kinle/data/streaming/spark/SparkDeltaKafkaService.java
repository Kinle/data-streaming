package me.kinle.data.streaming.spark;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.types.DataTypes.*;

@Slf4j
@Service
public class SparkDeltaKafkaService {

    private final SparkSession spark;
    private final KafkaProperties kafkaProperties;
    private final boolean BATCHING = true;

    public SparkDeltaKafkaService(@Autowired KafkaProperties kafkaProperties) {
        this.spark = SparkSession.builder().appName("KafkaSparkDeltaService").master("local[*]").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate();
        this.kafkaProperties = kafkaProperties;
    }

    public void readKafkaWriteDeltaAndCsv() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", StringUtils.join(kafkaProperties.getBootstrapServers(), ","))
                .option("enable.auto.commit", true)
                .option("subscribe", "test-topic")
                .option("startingOffsets", "earliest")
                .load();

        // Assuming value is a string, you may need to parse JSON if your data is structured

        StructType schema = new StructType()
                .add("id", LongType)
                .add("name", StringType)
                .add("description", StringType)
                .add("category", StringType)
                .add("tags", createArrayType(StringType))
                .add("createdDate", DateType)
                .add("updatedDate", DateType)
                .add("priceInfo", new StructType()
                        .add("price", DoubleType)
                        .add("currency", StringType)
                        .add("tax", DoubleType)
                        .add("shippingCost", DoubleType))
                .add("manufacturer", new StructType()
                        .add("name", StringType)
                        .add("address", StringType)
                        .add("contactNumber", StringType)
                        .add("email", StringType))
                .add("details", new StructType()
                        .add("material", StringType)
                        .add("color", StringType)
                        .add("size", StringType)
                        .add("usage", StringType)
                        .add("originCountry", StringType))
                .add("available", BooleanType)
                .add("stock", IntegerType)
                .add("weight", DoubleType)
                .add("dimensions", new StructType()
                        .add("length", DoubleType)
                        .add("width", DoubleType)
                        .add("height", DoubleType)
                        .add("unit", StringType))
                .add("ratings", createArrayType(DoubleType))
                .add("attributes", createMapType(StringType, StringType))
                .add("images", createArrayType(StringType))
                .add("barcode", StringType)
                .add("sku", StringType)
                .add("expiryDate", DateType)
                .add("discount", DoubleType);
        var stream = kafkaStream.selectExpr("CAST(value AS STRING) as value")
                .withColumn("json", from_json(col("value"), schema))
                .select("json.*")
                .limit(1_000_000);

        var writeStream = stream.writeStream()
                .trigger(Trigger.AvailableNow());
        writeStream
                .format("json")
                .option("checkpointLocation", "output/json/_checkpoint")
                .option("path", "output/json")
                .start()
                .awaitTermination();
        log.info("The process took {}", stopwatch.elapsed());
    }
}
