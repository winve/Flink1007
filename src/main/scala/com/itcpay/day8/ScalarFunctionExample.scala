package com.itcpay.day8

import com.itcpay.day1.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * 标量聚合函数 UDF
 * 将0、1或多个标量值，映射到新的标量值
 */
object ScalarFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource)

    val table: Table = tEnv.fromDataStream(stream, $"id", $"timestamp" as "ts", $"temperature")

    val hashCode = new HashCode(10)

    table
      .select($"id", hashCode($"id"))
      .toAppendStream[Row]
      .print("table")

    // sql 写法
    tEnv.createTemporarySystemFunction("hashCode", hashCode)
    tEnv.createTemporaryView("sensor", table)
    tEnv
      .sqlQuery("SELECT id, hashCode(id) FROM sensor")
      .toAppendStream[Row]
      .print("sql")

    env.execute()
  }

  class HashCode(factor: Int) extends ScalarFunction {
    def eval(s: String): Int = {
      s.hashCode * factor
    }
  }

}
