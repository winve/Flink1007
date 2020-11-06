package com.itcpay.day7

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

/**
 * table API
 */
object TableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 表环境配置
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // 使用blink planner，blink planner是流批统一
      .inStreamingMode()
      .build()

    // 初始化一个表环境
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // E:\Idea\2020\Flink1007\
    // 创建表
    tEnv
      .connect(new FileSystem().path("src\\main\\resources\\sensor.txt"))
      .withFormat(new Csv()) // 定义从外部系统读取数据之后的格式化方法
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ) // 定义表结构
      .createTemporaryTable("inputTable") // 创建临时表

    // 将临时表转换成Table数据类型
    val sensorTable: Table = tEnv.from("inputTable")

    // 使用Table API进行查询
    val tapiResult = sensorTable
      .select("id, temperature")
      .filter("id = 'sensor_1'")

    // 使用sql api进行查询
    val sqlResult = tEnv
      .sqlQuery("SELECT id, temperature FROM inputTable WHERE id = 'sensor_2'")

    tEnv.toAppendStream[Row](tapiResult).print()
    tEnv.toAppendStream[Row](sqlResult).print()

    env.execute()
  }
}
