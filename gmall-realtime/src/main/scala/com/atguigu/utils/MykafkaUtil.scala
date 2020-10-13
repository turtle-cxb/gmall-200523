package com.atguigu.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object MykafkaUtil {

  def getKafkaDstream(ssc : StreamingContext,topic : String) : InputDStream[ConsumerRecord[String, String]] = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    val broker_list: String = properties.getProperty("kafka.broker.list")
    val group_id: String = properties.getProperty("group.id")
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],//注意不要导错包否则会序列化失败
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true : java.lang.Boolean),
      ConsumerConfig.GROUP_ID_CONFIG -> group_id
    )
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams)
    )
    kafkaDstream

  }



}
