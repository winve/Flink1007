package com.itcpay.proj

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 实时流量统计
 * 统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
 */
object ApacheLogAnalysis {

  case class ApacheLogEvent(ip: String, eventTime: Long, method: String, url: String)

  case class UrlViewCount(url: String, windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("src/main/resources/apache.log")
      .map(line => {
        val arr = line.split(" ")
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = sdf.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), timestamp, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new CountAgg, new WindowResultCount)
      .keyBy(_.windowEnd)
      .process(new TopNHotUrl(5))
      .print()

    env.execute()
  }

  class TopNHotUrl(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    lazy val urlState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("url-state", Types.of[UrlViewCount])
    )

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allUrls = new ListBuffer[UrlViewCount]

      import scala.collection.JavaConverters._
      for (url <- urlState.get().asScala) {
        allUrls += url
      }
      urlState.clear()

      // 访问量从大到小排列
      val sortedUrlViews: ListBuffer[UrlViewCount] = allUrls
        .sortBy(_.count)(Ordering.Long.reverse) // 函数柯里化
        .take(topSize)

      val result = new StringBuilder

      result
        .append("====================================\n")
        .append("时间: ")
        .append(new Timestamp(timestamp - 1))
        .append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result
          .append("No")
          .append(i + 1)
          .append(": ")
          .append("  URL=")
          .append(currentUrlView.url)
          .append("  流量=")
          .append(currentUrlView.count)
          .append("\n")
      }
      result
        .append("====================================\n\n")

      out.collect(result.toString())
    }
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResultCount extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, context.window.getEnd, elements.head))
    }
  }

}
