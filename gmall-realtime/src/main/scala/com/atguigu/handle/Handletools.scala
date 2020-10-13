package com.atguigu.handle

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.Startuplog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object Handletools {
  //同批次去重
  def distinctCommonBatchDstream(dstream : DStream[Startuplog]) : DStream[Startuplog] ={
    val filterDstream: DStream[Startuplog] = dstream.map(startLog => {
      (startLog.mid, startLog)
    }).groupByKey()
      .flatMap {
        case (str, iter) => {
          iter.toList.sortWith((x, y) => x.logDate < y.logDate).take(1)
        }
      }
    filterDstream
  }

  private var sdf : SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  //跨批次去重
  def distinctAcrossBatch(dstream : DStream[Startuplog],ssc : StreamingContext):DStream[Startuplog] = {
    //第一种方法，效率低
    /*dstream.filter(startlog => {
      val redisClient: Jedis = RedisUtil.getJedisClient
      val boolean: lang.Boolean = redisClient.sismember(s"DAU:${startlog.logDate}",startlog.mid)
        redisClient.close()
      !boolean
    })*/

    //第二中方法
   /* val filterDstream: DStream[Startuplog] = dstream.mapPartitions(iter => {
      val redisClient: Jedis = RedisUtil.getJedisClient
      val startuplogs: Iterator[Startuplog] = iter.filter(startLog => {
        !redisClient.sismember(s"DAU:${startLog.logDate}", startLog.mid)

      })
      redisClient.close()
      startuplogs
    })
    filterDstream*/

    //第三种方法 广播变量

    val filterDstream: DStream[Startuplog] = dstream.transform(rdd => {
      val redisClient: Jedis = RedisUtil.getJedisClient
      val strings: util.Set[String] = redisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
      val setMembers: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(strings)
      redisClient.close()
      rdd.filter(startLog => {
        !setMembers.value.contains(startLog.mid)
      })
    })
    filterDstream

  }








  //将数据写入redis
  def writeRedis(dstream : DStream[Startuplog]) = {

    dstream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val redisClient: Jedis = RedisUtil.getJedisClient
        iter.foreach(startLog =>{
          redisClient.sadd(s"DAU:${startLog.logDate}",startLog.mid)
        })
        redisClient.close()
      })
    })

  }

}
