package com.itcpay.proj

import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
 * 自定义数据源
 */
class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehaviour] {

  var running: Boolean = true
  val random = new Random()
  val channelSet = Seq("AppleStore", "XiaomiStore", "HuaweiStore")
  val behaviourTypes = Seq("BROWSE", "CLICK", "INSTALL", "UNINSTALL")

  override def run(ctx: SourceContext[MarketingUserBehaviour]): Unit = {
    while (running) {
      val userId = UUID.randomUUID().toString
      val channel = channelSet(random.nextInt(channelSet.size))
      val behaviour = behaviourTypes(random.nextInt(behaviourTypes.size))
      val ts = Calendar.getInstance().getTimeInMillis
      ctx.collect(MarketingUserBehaviour(userId, behaviour, channel, ts))
      Thread.sleep(10)
    }
  }

  override def cancel(): Unit = running = false
}
