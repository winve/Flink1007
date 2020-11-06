package com.itcpay.day7

import com.itcpay.day1.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.types.Row

object TableEventTime {

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

    tEnv.fromDataStream(stream, $"id", $"timestamp".rowtime() as "ts", $"temperature" as "temp")
      .window(Slide over 10.seconds every 5.seconds on $"ts" as $"w")
      .groupBy($"id", $"w")
      .select($"id", $"id".count())
      .toAppendStream[Row]
      .print()

    env.execute()
  }

}
