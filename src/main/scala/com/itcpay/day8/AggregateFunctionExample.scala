package com.itcpay.day8

import com.itcpay.day1.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * 聚合函数 AggregateFunction 必须实现以下方法
 *  - createAccumulator()
 *  - accumulate()
 *  - getValue()
 */
object AggregateFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))

    val avgTemp = new AvgTemp

    val table: Table = tEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature" as "temp")

    // table
    table
      .groupBy($"id")
      .aggregate(avgTemp($"temp") as "avgTemp")
      .select($"id", $"avgTemp")
      .toRetractStream[Row]
      .print("table")

    // sql
    tEnv.createTemporaryView("sensor", table)

    tEnv.registerFunction("avgTemp", avgTemp)

    tEnv
      .sqlQuery(
        """
          |SELECT
          | id, avgTemp(temp)
          | FROM sensor
          | GROUP BY id
          |""".stripMargin)
      .toRetractStream[Row]
      .print("sql")

    env.execute()
  }

  // 累加器的类型
  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  // 第一个泛型是温度值的类型
  // 第二个泛型是累加器的类型
  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {

    // 创建累加器
    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    // 累加规则
    def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }

    // 计算并返回最终结果
    override def getValue(accumulator: AvgTempAcc): Double = {
      accumulator.sum / accumulator.count
    }

  }

}
