package com.itcpay.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val clickStream = env
      .fromElements(
        ("a", "click", 3600 * 1000L)
      )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)

    val browseStream = env
      .fromElements(
        ("a", "click", 2000 * 1000L),
        ("a", "click", 3000 * 1000L),
        ("b", "click", 3100 * 1000L),
        ("a", "click", 3200 * 1000L),
        ("a", "click", 4000 * 1000L),
        ("a", "click", 4100 * 1000L),
        ("a", "click", 7200 * 1000L)
      )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)

    clickStream
      .intervalJoin(browseStream)
      .between(Time.seconds(-600), Time.seconds(500))
      .process(new MyIntervalJoin)
      .print()

    env.execute()
  }

  class MyIntervalJoin extends ProcessJoinFunction[(String, String, Long), (String, String, Long), String] {
    override def processElement(left: (String, String, Long), right: (String, String, Long), ctx: ProcessJoinFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
      // left 来自第一条流； right 来自第二条流
      out.collect(left + " =====> " + right)
    }
  }

}
