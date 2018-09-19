package com.cdn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

object myCdnStatistics {

  val logger = LoggerFactory.getLogger(CdnStatistics.getClass)

  //匹配IP地址
  val IPPattern: Regex = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r
  val IPPatterm: Regex = "((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))".r

  //匹配视频文件名
  val videoPattern: Regex = "([0-9]+).mp4".r

  //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
  val timePattern: Regex = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  //匹配 http 响应码和请求数据大小
  val httpSizePattern: Regex = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("CdnStatistics"))

    val input = sc.textFile("D:\\Soft\\DevSoft\\IDEA\\spark\\sparkcore\\sparkcdn\\src\\main\\resources\\cdn.txt").cache()
    //IP      命中率     响应时间      请求时间      请求方法 请求URL 请求协议 状态码  响应大小     referer 用户代理
    //ClientIP Hit/Miss ResponseTime [RequestTime] Method URL Protocol StatusCode TrafficSize Referer UserAgent
    //111.19.97.15 HIT 18 [15/Feb/2017:00:00:39 +0800] "GET http://cdn.v.abc.com.cn/videojs/video-js.css HTTP/1.1" 200 14727 "http://www.zzqbsm.com/" "Mozilla/5.0+(Linux;+Android+5.1;+vivo+X6Plus+D+Build/LMY47I)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/35.0.1916.138+Mobile+Safari/537.36+T7/7.4+baiduboxapp/8.2.5+(Baidu;+P1+5.1)"

    //计算独立IP数   统计独立IP访问量前10位
//    ipStatistics(input)

    //统计每个视频独立IP数
//    videoIpStatistics(input)

    //统计一天中每个小时间的流量
    flowOfHour(input)

    sc.stop()
  }

  //count independent IP   | Output the number of IP access before the top 10
  def ipStatistics(data: RDD[String]): Unit = {
    data.map(x => (IPPattern.findFirstIn(x).get, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    //RegularExpression
    val independentIP = data
      .map(str => (str.substring(0, str.indexOf(" ")), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    //Output the number of IP access before the top 10
    independentIP.take(10).foreach(println)
    println("The number of independent IP : " + independentIP.count)
  }

  // 统计每个视频独立IP数
  def videoIpStatistics(data: RDD[String]): Unit = {
    //记得先过滤
    val numVideoIP = data
      .filter(x => x.matches(".*([0-9]+)\\.mp4.*"))
      .map(x => (videoPattern.findFirstIn(x).get, IPPattern.findFirstIn(x).get))
      .distinct
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    numVideoIP.take(10).foreach(x => println("video: " + x._1 + "\tIPnum: " + x._2))

    val numVideoIP2 = data
      .filter(x => x.matches(".*([0-9]+)\\.mp4.*"))
      .map(x => (videoPattern.findFirstIn(x).get, IPPattern.findFirstIn(x).get))
      .groupByKey()
      .map(x => (x._1, x._2.toList.distinct))
      .sortBy(_._2.size, false)

    numVideoIP2.take(10).foreach(x => println("video: " + x._1 + " \tIPnum: " + x._2.size))
    println("The number of video : " + numVideoIP2.count)
  }

//  100.79.121.48 HIT 33 [15/Feb/2017:00:00:46 +0800] "GET http://cdn.v.abc.com.cn/videojs/video.js HTTP/1.1" 200 174055 "http://www.abc.com.cn/" "Mozilla/4.0+(compatible;+MSIE+6.0;+Windows+NT+5.1;+Trident/4.0;)"

  //统计一天中每个小时间的流量
  def flowOfHour(data: RDD[String]): Unit = {
    val sortedRDD = data
      .filter(x => x.matches(".*\\s(200|206|304)\\s([0-9]+)\\s.*"))
      .filter(x => x.matches(".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*"))
      .map(x => (x.substring(x.indexOf("[") + 13, x.indexOf("[") + 15).toInt, "(200|206|304)\\s([0-9]+)".r.findFirstIn(x).get.substring(4).toLong))
      .reduceByKey(_ + _)
      .sortByKey()

//    println(sortedRDD.collect().mkString("\n"))
//    println(sortedRDD.mapPartitionsWithIndex((pNum, items) => Iterator(pNum + ": [" + items.mkString(",") + "]")).collect.mkString(" "))

    sortedRDD.take(24)/**要take才能按顺序排序*/.foreach(t => println(t._1 + " o'clock " + t._2.toLong / 1073741824 + "G"))

  }
}