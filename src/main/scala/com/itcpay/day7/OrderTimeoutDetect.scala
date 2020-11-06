package com.itcpay.day7

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 检测订单超时
 */
object OrderTimeoutDetect {

  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_2", "create", 3000L),
        OrderEvent("order_2", "pay", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    // 定义的规则
    val pattern = Pattern
      .begin[OrderEvent]("create")
      .where(_.eventType.equals("create"))
      .next("pay")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(stream, pattern)

    // 用来输出超时订单的信息
    // 超时订单的意思是只有create事件，没有pay事件
    val orderTimeoutOutputTag = new OutputTag[OrderEvent]("timeout")

    // 匿名函数，用来处理超时的检测
    val timeoutFunc = (map: scala.collection.Map[String, Iterable[OrderEvent]], ts: Long, out: Collector[OrderEvent]) => {
      val orderCreate = map.get("create").get.iterator.next()
      println("在 " + ts + "ms前未检测到订单支付！")
      out.collect(orderCreate)
    }

    // 匿名函数，用来处理支付成功的检查
    val selectFunc = (map: scala.collection.Map[String, Iterable[OrderEvent]], out: Collector[OrderEvent]) => {
      val createOrder = map("create").iterator.next()
      val orderPay = map.get("pay").get.iterator.next()
      out.collect(createOrder)
      out.collect(orderPay)
    }

    val detectStream = patternStream
      // flatSelect方法接收柯里化参数
      // 第一个参数：检测出的超时信息发送到侧输出标签
      // 第二个参数：用来处理超时信息的函数
      // 第三个参数：用来处理create和pay匹配成功的信息
      .flatSelect(orderTimeoutOutputTag)(timeoutFunc)(selectFunc)

    // 打印匹配成功的信息
    detectStream.print("success")
    // 打印超时信息
    detectStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute()
  }

}
