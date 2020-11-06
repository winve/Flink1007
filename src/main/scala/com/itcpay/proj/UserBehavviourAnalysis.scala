package com.itcpay.proj

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 每隔5分钟，统计最近1小时内热门商品
 * 热门度用浏览次数 `pv` 来衡量
 */
object UserBehavviourAnalysis {

  case class UserBehaviour(userId: Long,
                           itemId: Long,
                           categoryId: Int,
                           behaviour: String,
                           timestamp: Long
                          )

  case class ItemViewCount(itemId: Long, // 商品id
                           windowEnd: Long, // 窗口结束时间
                           count: Long) // itemId在windowEnd所属的窗口中被浏览的次数

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehaviour(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behaviour.equals("pv")) // 过滤出pv时间
      .assignAscendingTimestamps(_.timestamp) // 分配升序时间戳
      .keyBy(_.itemId) // 由于需要统计的是热门商品，所以使用itemId来进行分流
      .timeWindow(Time.hours(1), Time.minutes(5)) // 每隔5分钟，统计最近1小时
      // 增量聚合和全窗口聚合结合使用
      // 聚合结果ItemViewCount是每个窗口中每个商品被浏览的次数
      .aggregate(new CountAgg, new WindowResult)
      .keyBy(_.windowEnd)
      .process(new TopN(3))

    stream.print()

    env.execute()
  }

  class TopN(n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

    // 初始化一个列表状态变量
    lazy val itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-state", Types.of[ItemViewCount])
    )

    // 没来一条ItemViewCount就调用一次
    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      itemState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L)
    }

    // 定时器用来排序
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()
      // 导入隐式转换
      import scala.collection.JavaConverters._
      // 将列表状态变量中的元素都转移到allItems中
      // 因为列表状态变量没有排序的功能，所以必须取出来排序
      for (item <- itemState.get().asScala) {
        allItems += item
      }
      // 清空列表状态变量了，GC
      itemState.clear()

      // 对allItems降序排列，取出前n个元素
      val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(-_.count).take(n)

      // 打印结果
      val result = new StringBuilder
      result
        .append("============================================\n")
        .append("窗口结束时间是：")
        // 还原窗口结束时间，所以要减去100ms
        .append(new Timestamp(timestamp - 100L))
        .append("\n")
      for (i <- sortedItems.indices) {
        val currItem: ItemViewCount = sortedItems(i)
        result
          .append("第")
          .append(i + 1)
          .append("名的商品ID是：")
          .append(currItem.itemId)
          .append("，浏览量是：")
          .append(currItem.count)
          .append("\n")
      }
      result
        .append("============================================\n\n\n")

      out.collect(result.toString())
    }
  }

  class CountAgg extends AggregateFunction[UserBehaviour, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehaviour, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  // 全窗口聚合函数的输入就是增量聚合函数的输出
  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getEnd, elements.head))
    }
  }

}
