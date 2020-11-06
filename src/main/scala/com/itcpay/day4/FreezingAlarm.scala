package com.itcpay.day4

import com.itcpay.day1.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FreezingAlarm {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .process(new FreezingAlarmFunction)

    // 常规输出
    stream.print()
    // 侧输出
    stream.getSideOutput(new OutputTag[String]("freezing-alarm")).print()

    env.execute()
  }

  class FreezingAlarmFunction extends ProcessFunction[SensorReading, SensorReading] {

    lazy val freezingAlarmOut = new OutputTag[String]("freezing-alarm")

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 22) {
        ctx.output(freezingAlarmOut, s"${value.id}的传感器低温报警！")
      }
      // 发送常规输出
      out.collect(value)
    }
  }

}
