package org.example.flink

import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.example.kafka.KafkaUtils

/**
 * @author : cxm
 * @date : 2023/4/13 下午3:41
 * @Description : flink消费Kafka落redis主流程
 */
trait Kafka2RedisProcess {

  def init (properties: Properties): StreamExecutionEnvironment = {
    // 初始化运行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 配置加载到全局配置
    val configuration: Configuration = new Configuration
    val propSet = properties.entrySet()
    propSet.forEach (prop=>{
      val key = prop.getKey
      val value = prop.getValue
      if (key!=null && value!=null) {
        configuration.setString(String.valueOf(key), String.valueOf(value))
      }
    })
    // 设置checkpoint
    if ("true".equals(properties.getProperty("checkpoint.open"))) {
      val checkpointPath = properties.getProperty("stateBackend.save.dir")
      val stateBackendType = properties.getProperty("stateBackend.type")
      val checkpointInterval = properties.getProperty("checkpoint.interval").toLong
      val checkpointMinPause = properties.getProperty("checkpoint.min.pause").toLong
      val checkpointMode = properties.getProperty("checkpoint.mode")
      val asynchronousSnapshots = properties.getProperty("framework.asynchronousSnapshots").toBoolean
      val enableIncrementalCheckpointing = properties.getProperty("framework.enableIncrementalCheckpointing").toBoolean
      val checkpointFailOnError = properties.getProperty("checkpoint.fail.on.error").toBoolean

      if (checkpointPath.isEmpty || "MEMORYSTATEBACKEND".equalsIgnoreCase(checkpointMode)) {
        env.setStateBackend(new MemoryStateBackend().asInstanceOf[StateBackend])
      }
      if ("FS".equalsIgnoreCase(checkpointMode)) {
        env.setStateBackend(new FsStateBackend(checkpointPath,asynchronousSnapshots).asInstanceOf[StateBackend])
      }
      if ("ROCKSOB".equalsIgnoreCase(checkpointMode)) {
        env.setStateBackend(new RocksDBStateBackend(checkpointPath,enableIncrementalCheckpointing).asInstanceOf[StateBackend])
      }

      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(checkpointMinPause)
      env.getCheckpointConfig.setFailOnCheckpointingErrors(checkpointFailOnError)

      if ("EXACTLY_ONCE".equalsIgnoreCase(checkpointMode)) {
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE)
      } else {
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.AT_LEAST_ONCE)
      }
    }
    env
  }

  def source (env:StreamExecutionEnvironment, properties: Properties):Array[DataStream[String]] = {
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
    // 可以根据配置文件创建多个数据源
    Array(source)
  }

  def transform (env:StreamExecutionEnvironment, properties: Properties, dataStream: DataStream[String]*) : Array[DataStream[String]] = {
    // 对数据流具体的操作
    dataStream.toArray
  }

  def sink (properties: Properties, dataStream: DataStream[String]*):DataStreamSink[String] = {
    // 数据下沉
     dataStream.head.print()
  }
}
