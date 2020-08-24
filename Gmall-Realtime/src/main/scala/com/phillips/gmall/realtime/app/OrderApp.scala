package com.phillips.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.phillips.gmall.common.constants.GmallConstant
import com.phillips.gmall.realtime.bean.OrderInfo
import com.phillips.gmall.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("order_app")
        val ssc = new StreamingContext(conf,Seconds(5))

        val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

        // 变化结构  record => case class
        val orderInfoDstrearm: DStream[OrderInfo] = inputStream.map { record =>
            val jsonString: String = record.value()
            val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
            //日期
            val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = createTimeArr(0)
            val timeArr: Array[String] = createTimeArr(1).split(":")
            orderInfo.create_hour = timeArr(0)
            // 收件人 电话 脱敏
            orderInfo.consignee_tel = "*******" + orderInfo.consignee_tel.splitAt(7)._2
            orderInfo
        }

        orderInfoDstrearm.foreachRDD { rdd =>
            val configuration = new Configuration()
            println(rdd.collect().mkString("\n"))
            rdd.saveToPhoenix("GMALL_ORDER_INFO",
                Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), configuration,
                Some("hadoop102,hadoop103,hadoop104:2181"))
        }

        ssc.start()
        ssc.awaitTermination()
    }
}