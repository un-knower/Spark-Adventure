package com.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class AdClick(timestamp: Long, province: Int, city: Int, uid: Int, adId: Int)

object LogProcess {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("log").setMaster("local[*]"))
    //load data
    val logRDD: RDD[String] = sc.textFile("./log")

    /** 1516609143867 6 7 64 16
      * timestamp province city uid adId
      * AdClick 将数据转换成对象 */
    val adClickRDD: RDD[AdClick] = logRDD.map {
      x =>
        val paras = x.split(" ")
        AdClick(paras(0).toLong, paras(1).toInt, paras(2).toInt, paras(3).toInt, paras(4).toInt)
    }

    //统计每一个省份点击TOP3的广告ID
    demand1(adClickRDD)
    //统计每一个省份每一个小时的TOP3广告的ID
    demand2()

    sc.stop()

  }

  //统计每一个省份点击TOP3的广告ID
  def demand1(adClickRDD: RDD[AdClick]) = {

    //  (province_adId,1) 将RDD[AdClick]转换成 省份广告k,次数v RDD
    //将数据力度转换为 省份中的广告
    val proAndAd2Count: RDD[(String, Int)] = adClickRDD.map(adclick => (adclick.province + "_" + adclick.adId, 1))

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

    /*1、2、45*/
    //排序输出
    val result: RDD[String] = pro2Ads.flatMap {
      case (pro, items) =>
        //当前这个省份TOP3的广告集合
        val filterItems: Array[(Int, Int)] =
          items.toList.sortWith(_._2 < _._2).take(3).toArray
        val result = new ArrayBuffer[String]()
        for (item <- filterItems) {
          result += (pro + "、" + item._1 + "、" + item._2)
        }
        result
    }

    result.collect.foreach(println)

  }

  //统计每一个省份每一个小时的TOP3广告的ID
  def demand2() = {
    //    (省份+小时+广告，1)
    //    (省份+小时，Array(广告，sum))
  }
}