package com.itcpay.day7

import com.itcpay.day1.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object SQLEventTime {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    tEnv.createTemporaryView("sensor", stream, $"id", $"timestamp".rowtime() as "ts", $"temperature")

    tEnv
      .sqlQuery("SELECT id, COUNT(id) FROM sensor GROUP BY id, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)")
      .toAppendStream[Row]
      .print()

    env.execute()
  }

}
