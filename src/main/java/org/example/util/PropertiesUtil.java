package org.example.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

/**
 * @author : cxm
 * @Date : 2023/3/31 下午2:53
 * @Description : 配置文件工具类
 */
public class PropertiesUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);

    public static void main(String[] args) {
        PropertiesUtil propertiesUtil = new PropertiesUtil();
        String kafkaInputBroker = propertiesUtil.loadProperties(args[0]).getProperty("kafka.input.brokers");
        LOGGER.info(kafkaInputBroker);
    }

    public Properties loadProperties(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(
                    Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)),
                    StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOGGER.error(Arrays.toString(e.getStackTrace()));
        }
        return properties;
    }
}
