package com.itcpay.day8

import com.itcpay.day1.SensorSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * 表聚合函数TableAggregateFunction
 */
object TableAggregateFunctionExample {
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

    val top2Temp = new Top2Temp

    // table api
    val table: Table = tEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature" as "temp")

    table
      .groupBy($"id")
      .flatAggregate(top2Temp($"temp") as("t", "rank"))
      .select($"id", $"t", $"rank")
      .toRetractStream[Row]
      .print("table")

    // sql
    tEnv.createTemporaryView("t", table)

    tEnv.registerFunction("top2Temp", top2Temp)

//    tEnv
//      .sqlQuery(
//        """
//          |SELECT
//          | id, top2Temp(temp) AS topTemp
//          | FROM t
//          | GROUP BY id
//          |""".stripMargin)
//      .toRetractStream[Row]
//      .print("sql")

    env.execute()
  }

  // 累加器
  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
  }

  // 第一个泛型是输出：(温度值, 排名)
  // 第二个泛型是累加器
  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    // 函数名必须是accumulate
    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    // 函数名必须是emitValue，用来发射计算结果
    def emitValue(acc: Top2TempAcc, collector: Collector[(Double, Int)]): Unit = {
      collector.collect((acc.highestTemp, 1))
      collector.collect((acc.secondHighestTemp, 2))
    }
  }

}
