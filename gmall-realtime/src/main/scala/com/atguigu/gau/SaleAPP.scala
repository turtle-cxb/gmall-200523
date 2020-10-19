package com.atguigu.gau

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import collection.JavaConversions._

import scala.collection.mutable.ListBuffer

object SaleAPP {
  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleAPP").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //从kafka中消费数据
    val orderDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaDstream(ssc,GmallConstant.KAFKA_TOPIC_ORDER_INFO)
    val orderDetailDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaDstream(ssc,GmallConstant.KAFKA_TOPIC_ORDER_DETAIL_INFO)
    //转样例类并转化成元组
    val orderInfoDstream: DStream[(String, OrderInfo)] = orderDstream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      val date: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = date(0)
      orderInfo.create_hour=date(1).split(":")(0)
      orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0,4)+"****"+orderInfo.consignee_tel.substring(8)
      (orderInfo.id, orderInfo)
    })
    val orderDetailInfoDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      (orderDetail.order_id, orderDetail)
    })
    //双流进行join，并进行测试,并在CanalClient中将cananl发送到kafka的时间进行调整，我调整的是
    //发送到TOPIC_ORDER_DETAIL_INFO主题的数据，随机延迟，测试之后确实丢数据
//    val joinDstream: DStream[(String, (OrderDetail, OrderInfo))] = orderDetailInfoDstream.join(orderInfoDstream)
//    joinDstream.print()

    //为解决此问题，双流进行fullOuterJoin,并进行相应的处理，将丢失的数据找回来
    val saleJoinDstream: DStream[(String, (Option[OrderDetail], Option[OrderInfo]))] = orderDetailInfoDstream.fullOuterJoin(orderInfoDstream)
    val saleDetailStream: DStream[SaleDetail] = saleJoinDstream.mapPartitions(iter => {
      //由于需要返回值，将数据写入ES，所以要使用mapPartitions算子，此算子要返回值类型为Iterator
      //创建可变ListBuffer
      val saleDetails = new ListBuffer[SaleDetail]
      //创建redis连接
      val redisClient: Jedis = RedisUtil.getJedisClient
      iter.foreach { case (orderId, (orderDetail, orderInfo)) => {
        if (orderInfo.isDefined) {
          //再判断orderDetail是否为None,若不为None则join上数据，封装成saleDetail返回
          //若为None则说明orderDetail数据可能先到或者有延迟未到，则需要判断前一批次orderDetail存储在redis中的数据是否能join上
          //若有则封装成saleDetail返回
          //同时将orderInfo中的id存入到redis中，并存储一段时间（这段时间由网络最大延迟确定，若为5秒一批次，最大延迟为10秒，则最低存储15秒）防止后面还有晚到的orderDetail数据
          if (orderDetail.isDefined) {
            //①判断orderDetail是否为None,若不为None则join上数据，封装成saleDetail返回
            val saleDetail = new SaleDetail(orderInfo.get, orderDetail.get)
            saleDetails.append(saleDetail)
          } else {
            //②需要判断前一批次orderDetail存储在redis中的数据是否有数据
            if (redisClient.exists(s"orderDetail:${orderInfo.get.id}")) {
              val orderDetailSet: util.Set[String] = redisClient.smembers(s"orderDetail:${orderInfo.get.id}")
              for (orderDetailString <- orderDetailSet) {
                val orderDetailRedis: OrderDetail = JSON.parseObject(orderDetailString, classOf[OrderDetail])
                //封装成SaleDetail返回
                saleDetails.append(new SaleDetail(orderInfo.get, orderDetailRedis))

              }
            }

          }
          //③将orderInfo中的id存入到redis中，并存储一段时间（这段时间由网络最大延迟确定，若为5秒一批次，最大延迟为10秒，则最低存储15秒）防止后面还有晚到的orderDetail数据
          //JSON.toJSONString(orderInfo.get)，在scala中不能使用此方法将样例类转化成json字符串，需要导入隐式转换才可以
          import org.json4s.native.Serialization
          implicit val formats = org.json4s.DefaultFormats
          val orderInfoString: String = Serialization.write(orderInfo.get)
          redisClient.setex(s"order:${orderInfo.get.id}", 100, orderInfoString)
        } else {
          //若orderInfo为None，则说明orderDetail数据晚到或者先到，则需要获取redis里面key:order:orderId的数据，若有则封装成SaleDetail返回，说明晚到
          //若没有则说明先到，将自己存入redis
          if (redisClient.exists(s"order:${orderDetail.get.order_id}")) {

            val orderString: String = redisClient.get(s"order:${orderDetail.get.order_id}")
            //将获取到的数据转化成OrderInfo样例类并封装成SaleDetail返回
            val orderInfoRedis: OrderInfo = JSON.parseObject(orderString, classOf[OrderInfo])
            //将数据转化成SaleDetail存入saleDetails
            saleDetails.append(new SaleDetail(orderInfoRedis, orderDetail.get))

          } else {
            //不存在orderInfo数据则将自己存入redis
            import org.json4s.native.Serialization
            implicit val formats = org.json4s.DefaultFormats
            val orderDetailString: String = Serialization.write(orderDetail.get)
            redisClient.sadd(s"orderDetail:${orderDetail.get.order_id}", orderDetailString)
            redisClient.expire(s"orderDetail:${orderDetail.get.order_id}", 300)
          }


        }

      }
      }
      //关闭redis连接
      redisClient.close()
      saleDetails.toIterator
    })

    saleDetailStream.print()
    //开启ssc
    ssc.start()
    ssc.awaitTermination()
  }

}
