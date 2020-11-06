package com.itcpay.day2

import com.itcpay.day1.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighAndLowTemp {

  case class HighAndLowTemp(id: String, min: Double, max: Double, endTs: Long)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new HighAndLowAgg, new WindowResult)
      .print()

    env.execute()
  }

  class HighAndLowAgg extends AggregateFunction[SensorReading, (String, Double, Double), (String, Double, Double)] {
    override def createAccumulator(): (String, Double, Double) = ("", Double.MaxValue, Double.MinValue)

    override def add(in: SensorReading, acc: (String, Double, Double)): (String, Double, Double) = {
      (in.id, in.temperature.min(acc._2), in.temperature.max(acc._3))
    }

    override def getResult(acc: (String, Double, Double)): (String, Double, Double) = acc

    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = (a._1, a._2.min(b._2), a._3.max(b._3))
  }

  class WindowResult extends ProcessWindowFunction[(String, Double, Double), HighAndLowTemp, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[HighAndLowTemp]): Unit = {
      val minMaxTemp: (String, Double, Double) = elements.head
      out.collect(HighAndLowTemp(key, minMaxTemp._2, minMaxTemp._3, context.window.getEnd))
    }
  }

}
