package coreRDD

import org.apache.spark.{SparkConf, SparkContext}

object Actions extends App {
  val sc = new SparkContext(new SparkConf().setAppName("wordCount").setMaster("local[*]"))

  println("====== 1、reduce(f: (T, T) => T): T  如果最后不是返回RDD，那么就是行动操作，=================================")
  val rdd1 = sc.makeRDD(1 to 10, 2)
  println(rdd1.reduce(_ + _))
  //res0: Int = 55
  val rdd2 = sc.makeRDD(Array(("a", 1), ("a", 3), ("c", 3), ("d", 5)))
  println(rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2)))
  //res2: (String, Int) = (cdaa,12)

  println("====== 2、collect()  将RDD的数据返回到driver层进行输出  数据2G 客户端1G报错=================================")
  println("====== 3、count(): Long  计算RDD的数据数量，并输出=================================")
  println(rdd1.count)
  //res14: Long = 10
  println(rdd2.count)
  //res15: Long = 4

  println("====== 4、first()  返回RDD的第一个元素=================================")
  println(rdd2.first)
  //res16: (String, Int) = (a,1)

  println("====== 5、take(n) 返回RDD的前n个元素=================================")
  println(rdd2.take(2).mkString(", "))
  //res17: Array[(String, Int)] = Array((a,1), (a,3))

  println("====== 6、takeSample  采样=================================")
  println(rdd1.takeSample(true, 5).mkString(", "))
  //res39: Array[Int] = Array(7, 5, 7, 4, 4)

  println("====== 7、takeOrdered(n)  返回排序后的前几个数据=================================")
  val rdd7 = sc.makeRDD(Seq(10, 4, 2, 12, 3))
  println(rdd7.top(2).mkString(", "))
  //res41: Array[Int] = Array(12, 10)
  println(rdd7.takeOrdered(2).mkString(", "))
  //res42: Array[Int] = Array(2, 3)

  println("====== 8、aggregate (zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U) 聚合操作\tseqOp和combOp的初始值都是zeroValue=================================")
  val rdd8 = sc.makeRDD(1 to 6, 2)

  println(
    rdd8.aggregate(2)(
      { (x, y) => x + y }, //分区内
      { (a, b) => a + b } //分区间
    ))
  //res45: Int = 27=3*7 +6//两个分区内加2  分区间相加也加2

  println(
    rdd8.aggregate(2)(
      { (x: Int, y: Int) => x + y },
      { (a: Int, b: Int) => a * b }
    ))
  //res57: Int = 272  // (分区内2+1+2+3) * (分区内2+4+5+6)  *2

  println("====== 9、fold(zeroValue)(func)  aggregate的简化操作，seqOp和combOp相同=================================")
  println(rdd8.fold(2)(_ + _))
  //res51: Int = 27

  println("====== 10、saveAsTextFile（path）  path为HDFS相兼容的路径=================================")
  println("====== 11、saveAsSequenceFile（path） 将文件存储为SequenceFile=================================")
  println("====== 12、saveAsObjectFile（path） 将文件存储为ObjectFile=================================")
  println("====== 13、countByKey(): Map[K, Long]   返回每个Key的数据的数量。=================================")
  val rdd13 = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

  println(rdd13.countByKey())
  //res52: scala.collection.Map[Int,Long] = Map(3 -> 2, 1 -> 3, 2 -> 1)

  println("====== 14、foreach 对每一个元素进行处理。=================================")
  var rdd14 = sc.makeRDD(1 to 10, 2)
  var sum = sc.accumulator(0)

  rdd14.foreach(sum += _)

  println(sum.value)//有问题
  //res54: Int = 55

  rdd14.collect().foreach(print)

}
