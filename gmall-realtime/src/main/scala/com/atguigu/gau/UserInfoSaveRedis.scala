package com.atguigu.gau

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserInfoSaveRedis {
  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoSaveRedis")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //从kafka中获取数据
    val userInfoDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaDstream(ssc,GmallConstant.KAFKA_TOPIC_USER_INFO)
    userInfoDstream
      .map(record => {
      record.value()
    })
      .foreachRDD(rdd => {
      //创建redis连接
      rdd.foreachPartition(iter => {
        //创建redis连接
        val userRedisClient: Jedis = RedisUtil.getJedisClient
        //将数据写入redis
        iter.foreach(userStr => {
          val userInfo: UserInfo = JSON.parseObject(userStr,classOf[UserInfo])
          userRedisClient.set(s"userInfo:${userInfo.id}",userStr)
        })
        //关闭redis连接
        userRedisClient.close()
      })
    })
    //启动
    ssc.start()
    ssc.awaitTermination()
  }

}
