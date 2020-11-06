package com.itcpay.day6

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkCEPExample {

  case class LoginEvent(userId: String, eventType: String, ipAddr: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("user_1", "fail", "0.0.0.1", 1000L),
        LoginEvent("user_1", "fail", "0.0.0.2", 2000L),
        LoginEvent("user_1", "fail", "0.0.0.3", 3000L),
        LoginEvent("user_2", "success", "0.0.0.4", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)

    // 需要声明一个检测的规则
    val pattern = Pattern
      .begin[LoginEvent]("first") // 第一个事件命名为first
      .where(_.eventType.equals("fail")) // 第一个事件需要满足的条件
      .next("second") // 第二个时间命名为second，next表示第一个事件和第二个事件紧挨着
      .where(_.eventType.equals("fail")) // 第二个事件需要满足的条件
      .next("third") // 第三个事件
      .where(_.eventType.equals("fail"))
      .within(Time.seconds(10)) // 要求三个事件必须在10s之内连续发生

    CEP.pattern(stream, pattern)
      .select((pattern: scala.collection.Map[String, Iterable[LoginEvent]]) => {
        val first: LoginEvent = pattern.get("first").get.iterator.next()
        val second: LoginEvent = pattern.get("second").get.iterator.next()
        val third: LoginEvent = pattern.get("third").get.iterator.next()
        "用户：" + first.userId + "分别在ip " + first.ipAddr + "；" + second.ipAddr + "；" + third.ipAddr + " 登录失败！"
      })
      .print()

    env.execute()
  }

}
