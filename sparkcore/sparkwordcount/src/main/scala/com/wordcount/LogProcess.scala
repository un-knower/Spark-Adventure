package com.wordcount

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class AdClick(timeStamp: Long, province: Int, city: Int, uid: Int, adId: Int, hourStamp: String)

object LogProcess {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("log").setMaster("local[*]"))
    //load data
    val logRDD: RDD[String] = sc.textFile("E:\\Work\\TestData\\ProvinceTimeAd\\input\\provinceAd.log")

    /** 1536507575 6 7 64 16 10h
      * timestamp province city uid adId hourStamp
      * AdClick 将数据转换成对象 */
    val adClickRDD: RDD[AdClick] = logRDD.map {
      x =>
        val paras = x.split("\t")
        val hourStamp = new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date(paras(0).toLong))
        AdClick(paras(0).toLong, paras(1).toInt, paras(2).toInt, paras(3).toInt, paras(4).toInt, hourStamp)
    }

    //统计每一个省份点击TOP3的广告ID
    demand1(adClickRDD)
    println("------------------")
    //统计每一个省份每一个小时的TOP3广告的ID
    demand2(adClickRDD)

    sc.stop()

  }

  //统计每一个省份点击TOP3的广告ID
  def demand1(adClickRDD: RDD[AdClick]) = {
    //  (province_adId,1) 将RDD[AdClick]转换成 省份广告k,次数v RDD
    //将数据力度转换为 省份中的广告
    val proAndAd2Count: RDD[(String, Int)] = adClickRDD.map(adClick => (adClick.province + "_" + adClick.adId, 1))

    //  (province_adId,n) 计算每一个省份每一个广告的总点击量
    val proAndAd2Sum: RDD[(String, Int)] = proAndAd2Count.reduceByKey(_ + _)

    //  [province,(adId,n)] 将数据粒度转换为省份
    val pro2Ad: RDD[(Int, (Int, Int))] = proAndAd2Sum.map {
      case (proAndAd, sum) =>
        val para = proAndAd.split("_")
        //省份            (广告ID,  广告总数)
        (para(0).toInt, (para(1).toInt, sum))
    }

    //  [province,Iterable[(adId1,n),(adId2,m)...]
    //将一个省份中的所有广告聚集
    val pro2Ads: RDD[(Int, Iterable[(Int, Int)])] = pro2Ad.groupByKey()

    //1、2、45
    //排序输出
    val result: RDD[String] = pro2Ads.flatMap {
      case (pro, items) =>
        //当前这个省份TOP3的广告集合
        val filterItems: Array[(Int, Int)] = items.toList.sortWith(_._2 > _._2).take(3).toArray
        val buffer = new ArrayBuffer[String]()
        for (item <- filterItems) {
          buffer += (pro + "\t" + item._1 + "\t" + item._2)
        }
        buffer
    }
    //排序好看一点
    val prettyResult = result.sortBy(
      item => item.charAt(0)
    )
    prettyResult.collect.foreach(println)
//    prettyResult.saveAsTextFile("E:\\Work\\TestData\\ProvinceTimeAd\\output")
  }

  //统计每一个省份每一个小时的TOP3广告的ID
  def demand2(adClickRDD: RDD[AdClick]) = {
    //  (province_adId,1) 将RDD[AdClick]转换成 (省份+小时+广告)k,次数v RDD
    //将数据力度转换为 省份中的每小时的广告
    val proHourAd2Count: RDD[(String, Int)] = adClickRDD.map(adClick => (adClick.province + "_" + adClick.hourStamp + "_" + adClick.adId, 1))

    //  (province_adId,n) 计算每一个省份每一小时每一个广告的总点击量
    val proHourAd2Sum: RDD[(String, Int)] = proHourAd2Count.reduceByKey(_ + _)

    //  [province,(adId,n)] 将数据粒度转换为省份+小时
    val pro2Ad: RDD[((Int, String), (Int, Int))] = proHourAd2Sum.map {
      case (proHourAd, sum) =>
        val para = proHourAd.split("_")
        //(省份+小时)               (广告ID,  广告总数)
        ((para(0).toInt, para(1)), (para(2).toInt, sum))
    }

    //  [province,Iterable[(adId1,n),(adId2,m)...]
    //将一个省份一个小时中的所有广告聚集
    val proHour2Ads: RDD[((Int, String), Iterable[(Int, Int)])] = pro2Ad.groupByKey()

    /*1、2、45*/
    //排序输出
    val result: RDD[String] = proHour2Ads.flatMap {
      case (proHour, items) =>
        //当前这个省份TOP3的广告集合
        val filterItems: Array[(Int, Int)] = items.toList.sortWith(_._2 > _._2).take(2).toArray
        val buffer = new ArrayBuffer[String]()
        for (item <- filterItems) {
          buffer += (proHour._1 + "\t" + proHour._2 + "\t" + item._1 + "\t" + item._2)
        }
        buffer
    }
    //排序好看一点
    val prettyResult = result.sortBy(
      item => item.substring(0, 16)
    )

    prettyResult.collect.foreach(println)
//    prettyResult.saveAsTextFile("E:\\Work\\TestData\\ProvinceTimeAd\\outputHour")
  }
}