package com.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {
  //声明配置
  val sparkConf = new SparkConf().setAppName("wordCount")
    .setMaster("spark://hadoop102:7077")

  //创建SparkContext
  val sc = new SparkContext(sparkConf)

  //业务逻辑
  //  sc.textFile("hdfs://hadoop102:9000/sparks/README.md"
  //  ).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_
  //  ).saveAsTextFile("/sparks/out")

  val file = sc.textFile("hdfs://hadoop102:9000/sparks/README.md")

  val words = file.flatMap(_.split(" "))

  val word2count = words.map((_, 1))

  val result = word2count.reduceByKey(_ + _)

  result.saveAsTextFile("hdfs://hadoop102:9000/sparks/01wc2")

  //关闭Spark连接
  sc.stop()

}
/**机器学习
FPGrowth关联规则	啤酒和尿布
KMeans聚类			vip 黄金 白金 客户这些级别
线性回归			银行贷款 批 30k和300k
协同过滤推荐		电商推荐 网易云音乐每日推荐 猜你喜欢

没有神经网络		深度学习*/
