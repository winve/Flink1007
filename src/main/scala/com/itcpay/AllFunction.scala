package com.itcpay

import java.util.Properties

import com.alibaba.druid.pool.{DruidDataSource, DruidDataSourceFactory}
import com.itcpay.day1.SensorReading
import javax.sql.DataSource
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllFunction {

  def main(args: Array[String]): Unit = {
    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(new Properties())
    dataSource.getConnection
//    DruidDataSource
  }

  class MyProcessFunction extends ProcessFunction[String, String] {
    override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
      out.collect("none key process function")
    }
  }

  class MyKeyProcessFunction extends KeyedProcessFunction[String, String, String] {
    override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
      out.collect("keyed process function")
    }
  }

  class MyWindowProcessFunction extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("window process function")
    }
  }

}
