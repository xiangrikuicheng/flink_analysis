package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.kafka.KafkaUtils;
import org.example.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author : cxm
 * @date : 2023/4/6 上午9:49
 * @Description : 消费者
 */
public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        try {
            Properties properties = new PropertiesUtil().loadProperties(args[0]);
            KafkaConsumer<String, String> consumer = KafkaUtils.getJavaKafkaConsumer(properties);
            List<String> topics = Arrays.asList(properties.getProperty("kafka.output.topics").split(","));
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
                for (ConsumerRecord<String, String> r : records) {
                    String str = MessageFormat.format("Get success! topic: {0}, partition: {1}, offset: {2}, " +
                            "key: {3}, value: {4}\n", r.topic(), r.partition(), r.offset(), r.key(), r.value());
                    LOGGER.info(str);
                }
            }
        } catch (Exception e) {
            LOGGER.error(Arrays.toString(e.getStackTrace()));
        }
    }
}
