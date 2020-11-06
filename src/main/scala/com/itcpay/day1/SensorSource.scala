package com.itcpay.day1

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading] {

  // 数据源是否正常运行
  var running: Boolean = true

  override def run(ctx: SourceContext[SensorReading]): Unit = {
    val random = new Random

    var curFTemp = (1 to 10).map(
      i => ("sensor_" + i, random.nextGaussian() * 20))

    // 产生无限数据流
    while (running) {
      curFTemp = curFTemp.map(
        t => (t._1, t._2 + random.nextGaussian() * 0.5)
      )

      // 产生ms为单位的时间戳
      val curTime: Long = Calendar.getInstance().getTimeInMillis

      curFTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      // 每隔200ms发送一条数据
      Thread.sleep(200)
    }
  }

  // 定义当取消flink任务时，需要关闭数据源
  override def cancel(): Unit = running = false

}
