package com.itcpay.day1

import org.apache.flink.streaming.api.scala._

object ConsumerFromSensorReading {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.addSource(new SensorSource)

    dataStream.print()

    env.execute()
  }

}
