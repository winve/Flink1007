package com.itcpay.day5

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input1 = env
      .fromElements(
        ("a", 1, 1000L),
        ("a", 2, 2000L),
        ("b", 1, 3000L),
        ("b", 2, 4000L)
      )
      .assignAscendingTimestamps(_._3)

    val input2 = env
      .fromElements(
        ("a", 10, 1000L),
        ("a", 20, 2000L),
        ("b", 10, 3000L),
        ("b", 20, 4000L)
      )
      .assignAscendingTimestamps(_._3)

    input1
      .join(input2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new MyJoin)
      .print()

    env.execute()
  }

  class MyJoin extends JoinFunction[(String, Int, Long), (String, Int, Long), String] {
    override def join(first: (String, Int, Long), second: (String, Int, Long)): String = {
      first + " =====> " + second
    }
  }

}
