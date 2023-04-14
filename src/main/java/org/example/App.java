package org.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.example.kafka.KafkaUtils;
import org.example.redis.FlinkRedisSink;
import org.example.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

//import org.apache.flink.streaming.api.TimeCharacteristic;

/**
 * Hello world!
 */
public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try {
            // 初始化运行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            // 设置并行度
            env.setParallelism(1);
            // 加载配置文件
            Properties properties = new PropertiesUtil().loadProperties(args[0]);
            // 配置加载到全局配置
            Configuration configuration = new Configuration();
            Set<Map.Entry<Object, Object>> set = properties.entrySet();
            for (Map.Entry<Object, Object> objectObjectEntry : set) {
                Object key = objectObjectEntry.getKey();
                Object value = objectObjectEntry.getValue();
                if (key != null && value != null) {
                    configuration.setString(String.valueOf(key), String.valueOf(value));
                }
            }
            env.getConfig().setGlobalJobParameters(configuration);
            // 连接数据源
            DataStream<String> source = env.addSource(KafkaUtils.getFlinkKafkaConsumer(properties),
                    TypeInformation.of(String.class));
            source.print();
            // 数据结果下沉
            //source.addSink(KafkaUtils.getFlinkKafkaProducer(properties));
            source.addSink(new FlinkRedisSink());
            env.execute();
        } catch (Exception e) {
            LOGGER.error(Arrays.toString(e.getStackTrace()));
        }
    }
}
