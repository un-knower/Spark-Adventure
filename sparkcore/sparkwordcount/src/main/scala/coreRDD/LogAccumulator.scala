package coreRDD

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]] {
  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  // 检测是否为空
  override def isZero: Boolean = _logArray.isEmpty

  // 拷贝一个新的累加器
  override def copy(): org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized {
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }

  // 重置一个累加器
  override def reset(): Unit = _logArray.clear()

  // 每一个分区中用于添加数据的方法  小SUM
  override def add(v: String): Unit = _logArray.add(v)

  // 合并每一个分区的输出 总sum
  override def merge(other: org.apache.spark.util.AccumulatorV2[String, java.util.Set[String]]): Unit = {
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }
  }

  override def value: java.util.Set[String] = java.util.Collections.unmodifiableSet(_logArray)

}

// 过滤掉带字母的
object LogAccumulator {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("partittoner").setMaster("local[*]"))

    val accum = new LogAccumulator
    sc.register(accum, "logAccum")
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    for (v <- accum.value) print(v + "")
    println()
    sc.stop()
  }
}
