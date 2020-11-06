package com.itcpay.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 自定义迟到元素处理方式
 */
object RedirectLateEventCustom {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .process(new LateEventProc)

    stream.print()
    stream.getSideOutput(new OutputTag[String]("late")).print()

    env.execute()
  }

  class LateEventProc extends ProcessFunction[(String, Long), (String, Long)] {
    val late = new OutputTag[String]("late")

    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      if (value._2 < ctx.timerService().currentWatermark()) {
        ctx.output(late, "迟到事件来了！")
      } else {
        out.collect(value)
      }
    }
  }

}
