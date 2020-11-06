package com.itcpay.day8

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * TableFunction
 * 将0、1或多个标量值作为输入参数，返回任意数量的行作为输出
 */
object TableFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "hello#world",
        "itcpay#bigdata"
      )

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // table 写法
    val table = tEnv.fromDataStream(stream, $"s")

    val split = new Split("#")

    table
      .joinLateral(split($"s") as("word", "length"))
      .select($"s", $"word", $"length")
      .toAppendStream[Row]
      .print("table")

    tEnv.registerFunction("split", split)
    tEnv.createTemporaryView("t", table)

    tEnv
      // `T`的意思是元组，flink里面的固定语法
      .sqlQuery("SELECT s, world, length FROM t, LATERAL TABLE(split(s)) as T(world, length)")
      // .sqlQuery("SELECT s, world, length FROM t LEFT JOIN LATERAL TABLE(split(s)) as T(world, length) ON TRUE")
      .toAppendStream[Row]
      .print("sql")

    env.execute()
  }

  // 输出泛型是(String, Int)
  class Split(sep: String) extends TableFunction[(String, Int)] {
    def eval(s: String): Unit = {
      // 使用collect向下游发送数据
      s.split(sep).foreach(
        x => collect((x, x.length))
      )
    }
  }

}
