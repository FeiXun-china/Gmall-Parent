package com.phillips.gmall.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.phillips.gmall.common.constants.GmallConstant
import com.phillips.gmall.realtime.bean.StartUpLog
import com.phillips.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._
object DauApp {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_STARTUP, ssc)

        //        // 打印kafka中消费的数据
        //        inputDStream.foreachRDD { rdd =>
        //            println(rdd.map(_.value()).collect().mkString("\n"))
        //        }

        // 2、转换格式，同时补充两个时间字段
        val startUpLogDStream: DStream[StartUpLog] = inputDStream.map { record =>
            val line: String = record.value()
            val startUpLog: StartUpLog = JSON.parseObject(line, classOf[StartUpLog])
            val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
            val dateHour: String = simpleDateFormat.format(new Date(startUpLog.ts))
            val dataHourArr: Array[String] = dateHour.split(" ")
            startUpLog.logDate = dataHourArr(0)
            startUpLog.logHour = dataHourArr(1)
            startUpLog
        }

        // 处理复杂业务时添加cache()方法，增加缓存
        startUpLogDStream.cache()
        // 3、根据清单进行过滤  driver
        /**
         * 除了与其他批次之间的数据进行去重，还需要在同一批次中进行去重
         */
        // driver
        val filteredDStream: DStream[StartUpLog] = startUpLogDStream.transform { rdd =>
            // driver
            println("过滤前：" + rdd.count())
            val jedis: Jedis = RedisUtil.getJedisClient
            val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val dauKey = "dau:" + dateString
            val dauMidSet: util.Set[String] = jedis.smembers(dauKey)
            val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)

            val filteredRdd: RDD[StartUpLog] = rdd.filter { startUpLog => // executor
                val dauMidSet: util.Set[String] = dauMidBC.value
                val flag: lang.Boolean = dauMidSet.contains(startUpLog.mid)
                !flag
            }
            println("过滤后：" + filteredRdd.count())
            jedis.close()
            filteredRdd
        }

        // 相同批次的数据机型去重,同意批次内，相同mid只保留第1条
        val filteredDStreamByMid: DStream[(String, StartUpLog)] = filteredDStream.map { startUpLog => (startUpLog.mid, startUpLog) }
        val startUpLogDStreamGroupByMid: DStream[(String, Iterable[StartUpLog])] = filteredDStreamByMid.groupByKey()
        val startUpLogDStreamRealFiltered: DStream[StartUpLog] = startUpLogDStreamGroupByMid.flatMap { case (mid, startUpLogItr) =>
            val top1List: List[StartUpLog] = startUpLogItr.toList.sortWith { (s1, s2) =>
                s1.ts < s2.ts
            }.take(1)
            top1List
        }

        // 4、将用户访问清单保存到redis中  // driver executor
        startUpLogDStreamRealFiltered.foreachRDD { rdd =>
            // driver
            // 通过 foreachPartition 函数性能更好，也避免了redis对象无法序列化的问题，将redis放在循环外，
            // 减少连接时性能的开销
            rdd.foreachPartition { startupItr =>
                // executor
                // val jedis: Jedis = new Jedis("hadoop102", 6379)
                val jedis: Jedis = RedisUtil.getJedisClient
                for (startUpLog <- startupItr) {
                    // executor
                    val dauKey = "dau:" + startUpLog.logDate
                    jedis.sadd(dauKey, startUpLog.mid)
                }
                jedis.close()
            }

            //            rdd.foreach { startUpLog =>
            //                // executor
            //                // 保存redis的操作  所有今天访问过的mid
            //                /* redis:
            //                    1、type=？set,利用该数据结构自动去重
            //                    2、key=？[dau:2020-08-19]
            //                    3、value=？[mid]
            //                    常用的5种redis类型：string、list、set、zset、hash
            //                 */
            //                val dauKey = "dau:" + startUpLog.logDate
            //                jedis.sadd(dauKey, startUpLog.mid)
            //            }

        }

        // 通过phoenix将流数据保存进入hbase中
        startUpLogDStreamRealFiltered.foreachRDD{ rdd =>
            rdd.saveToPhoenix("GMALL_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
                new Configuration,
                Some("hadoop102,hadoop103,hadoop104:2181"))

        }

        ssc.start()
        ssc.awaitTermination()
    }
}
