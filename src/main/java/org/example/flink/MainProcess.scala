package org.example.flink

import org.example.util.PropertiesUtil

/**
 * @author : cxm
 * @date : 2023/4/14 上午11:28
 * @Description : 程序运行入口
 */
object MainProcess {
  def main (args: Array[String]) : Unit = {
    if (args.length<2) {

    }
    // 加载配置
    val propertiesUtil = new PropertiesUtil()
    val properties = propertiesUtil.loadProperties(args(0))
    val processName = args(1)
    val ClassName = properties.getProperty("framework.streaming.className").trim
    val processImp = Class.forName(ClassName).newInstance().asInstanceOf[Kafka2RedisProcess]

    // 建立处理主流程
    val env = processImp.init(properties)
    val sourceDataStreams = processImp.source(env, properties)
    val resultDataStreams = processImp.transform(env, properties, sourceDataStreams:_*)
    processImp.sink(properties, resultDataStreams:_*)
    val appName = properties.getProperty("framework.streaming.appName").trim
    env.execute(appName)
  }
}
