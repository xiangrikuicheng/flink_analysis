package org.example.process

import java.util.Properties

import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.example.flink.Kafka2RedisProcess
import org.example.kafka.KafkaUtils

/**
 * @author : cxm
 * @date : 2023/4/14 下午3:54
 * @Description : 
 */
class ProcessKafka2redis extends Kafka2RedisProcess with Serializable {
  override def source(env:StreamExecutionEnvironment, properties: Properties):Array[DataStream[String]] = {
    val props = new Properties()
    props.setProperty("kafka.input.brokers", properties.getProperty("kafka.input.brokers"))
    props.setProperty("kafka.input.groupId", properties.getProperty("kafka.input.groupId"))
    props.setProperty("kafka.input.enable.auto.commit", properties.getProperty("kafka.input.enable.auto.commit"))
    props.setProperty("kafka.input.auto.commit.interval.ms", properties.getProperty("kafka.input.auto.commit.interval.ms"))
    props.setProperty("kafka.input.isolation.level", properties.getProperty("kafka.input.isolation.level"))
    props.setProperty("kafka.input.flink.partition-discovery.interval-millis", properties.getProperty("kafka.input.flink.partition-discovery.interval-millis"))
    props.setProperty("kafka.input.auto.offset.reset", properties.getProperty("kafka.input.auto.offset.reset"))
    props.setProperty("kafka.input.topics", properties.getProperty("kafka.input.topics"))
    props.setProperty("kafka.input.ssl", properties.getProperty("kafka.input.ssl"))
    props.setProperty("kafka.input.user", properties.getProperty("kafka.input.user"))
    props.setProperty("kafka.input.pass", properties.getProperty("kafka.input.pass"))
    val source = env.addSource(KafkaUtils.getFlinkKafkaConsumer(props))
    // 可以根据不同的配置文件创建多个数据源
    val props1 = new Properties()
    props1.setProperty("kafka.input.brokers", properties.getProperty("kafkaTest.input.brokers"))
    props1.setProperty("kafka.input.groupId", properties.getProperty("kafkaTest.input.groupId"))
    props1.setProperty("kafka.input.enable.auto.commit", properties.getProperty("kafkaTest.input.enable.auto.commit"))
    props1.setProperty("kafka.input.auto.commit.interval.ms", properties.getProperty("kafkaTest.input.auto.commit.interval.ms"))
    props1.setProperty("kafka.input.isolation.level", properties.getProperty("kafkaTest.input.isolation.level"))
    props1.setProperty("kafka.input.flink.partition-discovery.interval-millis", properties.getProperty("kafkaTest.input.flink.partition-discovery.interval-millis"))
    props1.setProperty("kafka.input.auto.offset.reset", properties.getProperty("kafkaTest.input.auto.offset.reset"))
    props1.setProperty("kafka.input.topics", properties.getProperty("kafkaTest.input.topics"))
    props1.setProperty("kafka.input.ssl", properties.getProperty("kafkaTest.input.ssl"))
    props1.setProperty("kafka.input.user", properties.getProperty("kafkaTest.input.user"))
    props1.setProperty("kafka.input.pass", properties.getProperty("kafkaTest.input.pass"))
    val source1 = env.addSource(KafkaUtils.getFlinkKafkaConsumer(props1))
    Array(source)
  }

  override def transform (env:StreamExecutionEnvironment, properties: Properties, dataStream: DataStream[String]*) : Array[DataStream[String]] = {
    // 对数据流具体的操作
    dataStream.toArray
  }

  override def sink (properties: Properties, dataStream: DataStream[String]*):DataStreamSink[String] = {
    // 数据下沉
    dataStream.head.print()
  }

}
