package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.KafkaUtils;
import org.example.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author : cxm
 * @date : 2023/4/4 下午3:56
 * @Description : 生产者
 */
public class Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws Exception {
        Properties properties = new PropertiesUtil().loadProperties(args[0]);
        KafkaProducer<String, String> producer = KafkaUtils.getJavaKafkaProducer(properties);
        List<String> topics = Arrays.asList(properties.getProperty("kafka.output.topics").split(","));
        if (!topics.isEmpty()) {
            for (int i = 0; i < 5; i++) {
                String value = "message_" + i + "->" + LocalDateTime.now();
                LOGGER.info("Send value: {}", value);
                producer.send(new ProducerRecord<>(topics.get(0), value), (metadata, exception) -> {
                    if (exception == null) {
                        String str = MessageFormat.format("Send success! topic: {0}, partition: {1}, offset: {2}\n", metadata.topic(), metadata.partition(), metadata.offset());
                        LOGGER.info(str);
                    }
                });
                Thread.sleep(500);
            }
        }
        producer.close();
    }
}
