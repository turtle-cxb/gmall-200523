package com.atguigu.gau

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.utils.{MykafkaUtil, PropertiesUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {
  def main(args: Array[String]): Unit = {
    //创建StreamContext
    val sparkConf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //从kafka中消费数据
    val orderDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaDstream(ssc,"TOPIC_ORDER_INFO")
    val orderInfoDstream: DStream[OrderInfo] = orderDstream.map(record => {
      val orderRecord: String = record.value()
      //将数据转化成样例类
      val orderInfo: OrderInfo = JSON.parseObject(orderRecord, classOf[OrderInfo])
      //为create_date,create_hour字段赋值
      val date: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = date(0)
      val hourDate: Array[String] = date(1).split(":")
      orderInfo.create_hour = hourDate(0)
      //脱敏
      val str1: String = orderInfo.consignee_tel.substring(0, 4)
      val str2: String = orderInfo.consignee_tel.substring(7)
      orderInfo.consignee_tel = str1 + "****" + str2
      orderInfo
    })
    orderInfoDstream.cache()
    orderInfoDstream.count().print()
    orderInfoDstream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        PropertiesUtil.load("config.properties").getProperty("phoenix.orderInfo.table"),
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        new HBaseConfiguration(),
        Some(PropertiesUtil.load("config.properties").getProperty("phoenix.zk.url"))
      )
    })

    ssc.start()
    ssc.awaitTermination()


  }

}
