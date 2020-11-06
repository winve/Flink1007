package com.itcpay.day7

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 使用DateStream API检测订单超时事件
 */
object OrderTimeoutWithoutCEP {

  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_1", "pay", 3000L),
        OrderEvent("order_2", "create", 3000L),
        OrderEvent("order_2", "pay", 4000L),
        OrderEvent("order_3", "create", 4000L),
        OrderEvent("order_4", "create", 5000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)
      .process(new MatchFunction)

    stream.print()

    env.execute()
  }

  class MatchFunction extends KeyedProcessFunction[String, OrderEvent, String] {

    var orderState: ValueState[OrderEvent] = _

    override def open(parameters: Configuration): Unit = {
      orderState = getRuntimeContext.getState(
        new ValueStateDescriptor[OrderEvent]("save-order", Types.of[OrderEvent])
      )
    }

    override def processElement(event: OrderEvent, ctx: KeyedProcessFunction[String, OrderEvent, String]#Context, out: Collector[String]): Unit = {
      if (event.eventType.equals("create")) {
        if (orderState.value() == null) {
          // 保存create事件
          orderState.update(event)
          ctx.timerService().registerEventTimeTimer(event.eventTime + 5000L)
        }
      } else {
        // 保存pay事件
        out.collect("已经支付的订单是：" + event.orderId)
        orderState.update(event)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val saveOrder = orderState.value()

      if (saveOrder != null && saveOrder.eventType.equals("create")) {
        out.collect("超时订单ID是 " + saveOrder.orderId)
      }

      orderState.clear()
    }
  }

}
