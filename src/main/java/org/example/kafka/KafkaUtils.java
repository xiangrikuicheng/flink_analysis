package org.example.kafka;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author : cxm
 * @date : 2023/3/31 下午4:32
 * @Description : kafka工具类
 */
public class KafkaUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    public static Properties getConsumerConfig(Properties properties) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.input.brokers"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("kafka.input.groupId"));
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                properties.getProperty("kafka.input.enable.auto.commit"));
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                properties.getProperty("kafka.input.auto.commit.interval.ms"));
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, properties.getProperty("kafka.input.isolation.level"));
        props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                properties.getProperty("kafka.input.flink.partition-discovery.interval-millis"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty("kafka.input.auto.offset.reset"));
        //是否为加密模式
        if ("true".equals(properties.getProperty("kafka.input.ssl"))) {
            props.setProperty("security.protocol", "SASL_PLAINTEXT");
            props.setProperty("sasl.mechanism", "PLAIN");
            String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" " +
                    "password=\"{1}\";";
            props.setProperty("sasl.jaas.config", MessageFormat.format(jaasTemplate,
                    properties.getProperty("kafka.input.user"), properties.getProperty("kafka.input.pass")));
            LOGGER.info("Kafka is in state of ssl");
        }
        return props;
    }

    public static Properties getProducerConfig(Properties properties) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.output.brokers"));
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, properties.getProperty("kafka.output.transaction.timeout"));
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                properties.getProperty("kafka.output.max.request.size"));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                properties.getProperty("kafka.output.compression.type"));
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, properties.getProperty("kafka.output.batch.size"));
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, properties.getProperty("kafka.output.buffer.memory"));
        //是否为加密模式
        if ("true".equals(properties.getProperty("kafka.output.ssl"))) {
            props.setProperty("security.protocol", "SASL_PLAINTEXT");
            props.setProperty("sasl.mechanism", "PLAIN");
            String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" " +
                    "password=\"{1}\";";
            props.setProperty("sasl.jaas.config", MessageFormat.format(jaasTemplate,
                    properties.getProperty("kafka.output.user"), properties.getProperty("kafka.output.pass")));
            LOGGER.info("Kafka is in state of ssl");
        }
        return props;
    }

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(Properties properties) {
        Properties props = getConsumerConfig(properties);
        SimpleStringSchema sss = new SimpleStringSchema();
        List<String> topics = Arrays.asList(properties.getProperty("kafka.input.topics").split(","));
        return new FlinkKafkaConsumer<>(topics, sss, props);
    }

    public static KafkaConsumer<String, String> getJavaKafkaConsumer(Properties properties) {
        Properties props = getConsumerConfig(properties);
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    public static <T> FlinkKafkaConsumer<T> getKafkaConsumerAvro(Properties properties, Class<T> clz) {
        Properties props = getConsumerConfig(properties);
        //kafka反序列化对象
        KafkaDeserializationSchema<T> deserializationSchema = new KafkaDeserializationSchema<T>() {
            @Override
            public boolean isEndOfStream(T t) {
                return false;
            }

            @Override
            public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                ReflectDatumReader<T> reflectDatumReader = new ReflectDatumReader<>(ReflectData.AllowNull.get().getSchema(clz));
                return reflectDatumReader.read(null, DecoderFactory.get().binaryDecoder(consumerRecord.value(), null));
            }

            @Override
            public TypeInformation<T> getProducedType() {
                return TypeInformation.of(clz);
            }
        };
        List<String> topics = Arrays.asList(properties.getProperty("kafka.input.topics").split(","));
        return new FlinkKafkaConsumer<T>(topics, deserializationSchema, props);
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(Properties properties) {
        Properties props = getProducerConfig(properties);
        SimpleStringSchema sss = new SimpleStringSchema();
        List<String> topics = Arrays.asList(properties.getProperty("kafka.output.topics").split(","));
        if (!topics.isEmpty()) {
            return new FlinkKafkaProducer<>(topics.get(0), sss, props);
        } else {
            return null;
        }
    }

    public static KafkaProducer<String, String> getJavaKafkaProducer(Properties properties) {
        Properties props = getProducerConfig(properties);
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducerAvro(Properties properties, Class<T> clz) {
        Properties props = getConsumerConfig(properties);
        List<String> topics = Arrays.asList(properties.getProperty("kafka.output.topics").split(","));
        if (!topics.isEmpty()) {
            // 序列化对象
            KafkaSerializationSchema<T> serializationSchema = new KafkaSerializationSchema<T>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(T t, @Nullable Long aLong) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                    ReflectDatumWriter<T> writer = new ReflectDatumWriter<>(ReflectData.AllowNull.get().getSchema(clz));
                    byte[] value = null;
                    try {
                        writer.write(t, encoder);
                        encoder.flush();
                        value = out.toByteArray();
                        out.close();
                    } catch (Exception e) {
                        LOGGER.error(Arrays.toString(e.getStackTrace()));
                    }
                    return new ProducerRecord<>(topics.get(0), value);
                }
            };
            return new FlinkKafkaProducer<T>(topics.get(0), serializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else {
            return null;
        }

    }
}
