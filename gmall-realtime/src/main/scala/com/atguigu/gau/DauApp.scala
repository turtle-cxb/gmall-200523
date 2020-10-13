package com.atguigu.gau

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.Startuplog
import com.atguigu.constants.GmallConstant
import com.atguigu.handle.Handletools
import com.atguigu.utils.MykafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall20201013")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaDstream(ssc,GmallConstant.KAFKA_TOPIC_START)
    //将json数据转化成样例类
    val startUpLogDstream: DStream[Startuplog] = kafkaDstream.map(data => {
      val str: String = data.value()
      val startuplog: Startuplog = JSON.parseObject(str, classOf[Startuplog])
      val date: String = sdf.format(new Date(startuplog.ts))
      val dateArray: Array[String] = date.split(" ")
      startuplog.logDate = dateArray(0)
      startuplog.logHour = dateArray(1)
      startuplog
    })
    startUpLogDstream.cache()
    startUpLogDstream.count().print()

    //跨批次去重
    val distinctAcrossBatchDstream: DStream[Startuplog] = Handletools.distinctAcrossBatch(startUpLogDstream,ssc)

    distinctAcrossBatchDstream.cache()
    distinctAcrossBatchDstream.count().print()

    //同批次去重
    val dCommonBatchDstream: DStream[Startuplog] = Handletools.distinctCommonBatchDstream(distinctAcrossBatchDstream)
    dCommonBatchDstream.cache()
    dCommonBatchDstream.count().print()
    //数据写入redis
    Handletools.writeRedis(dCommonBatchDstream)

    //数据写入Phoenix
    dCommonBatchDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix(
        "GMALL2020_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,
        new Configuration,
        Some("hadoop107,hadoop108,hadoop109:2181"))
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
