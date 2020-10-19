package com.atguigu.gau

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyEsUtil, MykafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object WarningApp {
  /*需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品
  * 同一设备（分组）
5分钟内（窗口）
三次不同账号登录（用户去重）
领取优惠券（行为）
没有浏览商品（行为）
同一设备每分钟只记录一次预警（ES去重）
  *
  * */
  def main(args: Array[String]): Unit = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    //创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WarningApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //从kafka中消费数据，topic为TOPIC_EVENT，并将数据转化成EventLog类,然后将数据类型转成（mid,eventLog）
    val eventDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaDstream(ssc, GmallConstant.KAFKA_TOPIC_EVENT)
    val toupDstream: DStream[(String, EventLog)] = eventDstream.map(record => {
      val event: String = record.value()
      val eventLog: EventLog = JSON.parseObject(event, classOf[EventLog])
      val date: Array[String] = sdf.format(new Date(eventLog.ts)).split(" ")
      eventLog.logDate=date(0)
      eventLog.logHour=date(1)
      (eventLog.mid, eventLog)
    })
    //对mid进行分组并进行5分钟的开窗
    val windowDstream: DStream[(String, Iterable[EventLog])] = toupDstream.window(Minutes(5)).groupByKey()
    //
    //判断iterable中的eventLog中的uid个数是否超过或等于3，看事件类型是否有点击商品行为，若uid超过或者等于3且无点击商品行为
    //则需要生成预警日志
    val booleanToWarningDstream: DStream[(Boolean, CouponAlertInfo)] = windowDstream.map { case (mid, iter) => {
      //创建一个HashSet来对uid进行去重，方便统计uid数量
      val uids = new util.HashSet[String]()
      //创建一个标签来判断是否浏览过商品
      var noClick: Boolean = true
      //创建一个HashSet对被领取过优惠劵商品id进行封装并去重
      val itemIds = new util.HashSet[String]()
      //创建一个List对事件进行封装，个人感觉用HashSet会更好些，可以去重
      val events = new util.ArrayList[String]()


      breakable {
        iter.foreach(eventLog => {
          //统计该mid所有的事件，就是加购物车，浏览商品等等
          events.add(eventLog.evid)

          //这里由于一条事件日志只能有一个行为比如评论，收藏，领优惠劵，所以可以使用else if ,
          // if也可以使用，不过就会多一些判断
          if ("coupon".equals(eventLog.evid)) {
            uids.add(eventLog.uid)
            itemIds.add(eventLog.itemid)
          } else if ("clickItem".equals(eventLog.evid)) {
            noClick = false
            break()
          }
        })
      }
      //封装成二元组，但这个预警日志还包括没有领优惠劵及uid个数没有超过或等于3的数据
      (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }
    }
    //通过过滤来获取真正的预警日志，过滤条件为uids.size() >= 3 && noClick为真，由上面的noClick可以看出noClick为false
    //时，说明这个设备浏览过商品，不符合需求
    booleanToWarningDstream.cache()
    booleanToWarningDstream.count().print()

    val warningInfoDstream: DStream[CouponAlertInfo] = booleanToWarningDstream.filter(_._1).map(_._2)

    warningInfoDstream.cache()
    warningInfoDstream.count().print()

    //将预警日志写入ES，注意：写入ES时，预警日志字段对应的类型为java的集合类型，因为ES中的数据类型是与java类型对接的
    val dfs = new SimpleDateFormat("yyyy-MM-dd")
    val hourDfs = new SimpleDateFormat("HH:mm")
    warningInfoDstream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val hourMinu: String = hourDfs.format(new Date(System.currentTimeMillis()))
        //写入ES，实现一分钟相同的mid只预警一次，根据doc_id来进行去重，es的幂等性
        val tuplesList: List[(String, CouponAlertInfo)] = iter.toList.map(warningInfo => {
          (s"${warningInfo.mid}-${hourMinu}", warningInfo)
        })

        MyEsUtil.insertBulk(
          s"${PropertiesUtil.load("config.properties").getProperty("es.warning.prefix")}-${dfs.format(new Date(System.currentTimeMillis()))}",
          tuplesList
        )
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }

}
