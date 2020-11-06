package com.itcpay.day7

import com.itcpay.day1.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object TableFromDataStreamExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    // 字段名必须以`'`开始，as用来取别名
    // DataStream => Table
    val table = tEnv.fromDataStream(stream, 'id, 'timestamp as 'ts, 'temperature)

    val sTable: Table = table
      .select($"id")

    tEnv.toAppendStream[Row](sTable).print()

    // 使用数据流来创建名为sensor的表
    tEnv.createTemporaryView("sensor", stream, $"id", $"timestamp", $"temperature")

    val tSql: Table = tEnv
      .sqlQuery("select * from sensor where id = 'sensor_1'")
    tEnv.toRetractStream[Row](tSql).print("sql")

    // DataStream => Table => CURD => Table => DataStream
    env.execute()
  }

}
