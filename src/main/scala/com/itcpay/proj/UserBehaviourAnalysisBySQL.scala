package com.itcpay.proj

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * 实时热门商品统计
 * Flink SQL
 */
object UserBehaviourAnalysisBySQL {

  case class UserBehaviour(userId: Long,
                           itemId: Long,
                           categoryId: Int,
                           behaviour: String,
                           timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehaviour(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behaviour.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.createTemporaryView("t", stream, $"itemId", $"timestamp".rowtime() as "ts")

    // HOP_END是关键字，用来获取窗口结束时间
    // 最内层的子查询相当于 stream.keyBy(_.itemId).timeWindow(滑动窗口).aggregate()
    // 倒数第二层的子查询相当于 .keyBy(_.windowEnd).process(排序)
    // 最外层查询相当于 .take(3)
    tEnv
      .sqlQuery(
        """
          |SELECT *
          |FROM (
          |       SELECT *,
          |           ROW_NUMBER() OVER(PARTITION BY windowEnd ORDER BY itemCount DESC) AS row_num
          |       FROM (
          |         SELECT itemId, COUNT(itemId) AS itemCount,
          |             HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd
          |         FROM t GROUP BY itemId, HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)
          |            )
          |     )
          |WHERE row_num <= 3
          |""".stripMargin)
      .toRetractStream[Row]
      .filter(_._1 == true)
      .print()

    env.execute()
  }

}
