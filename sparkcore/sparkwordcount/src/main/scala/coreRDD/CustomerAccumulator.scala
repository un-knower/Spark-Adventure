package coreRDD

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class CustomerAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  private val _hashAcc = new mutable.HashMap[String, Int]()

  // 检测是否为空
  override def isZero: Boolean = {
    _hashAcc.isEmpty
  }

  // 拷贝一个新的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new CustomerAccumulator()
    _hashAcc.synchronized {
      newAcc._hashAcc ++= _hashAcc
    }
    newAcc
  }

  // 重置一个累加器
  override def reset(): Unit = {
    _hashAcc.clear()
  }

  // 每一个分区中用于添加数据的方法 小SUM
  override def add(k: String): Unit = {
  //_hashAcc += (k -> (_hashAcc.getOrElse(k, 0) + 1))
    _hashAcc.get(k) match {
      case None => _hashAcc += ((k, 1))
      case Some(a) => _hashAcc += ((k, a + 1))
    }
  }

  // 合并每一个分区的输出 总sum
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    for ((k, v) <- other.value) {
    //_hashAcc += (k -> (_hashAcc.getOrElse(k, 0) + v))
      _hashAcc.get(k) match {
        case None => _hashAcc += ((k, v))
        case Some(a) => _hashAcc += ((k, a + v))
      }
    }
  }

  // 输出值
  override def value: mutable.HashMap[String, Int] = {
    _hashAcc
  }
}

object CustomerAccumulator {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("partittoner").setMaster("local[*]"))

    //本地变量 每个分区中有一个copy,一万个分区就一万个copy
    val abc = "broadcast"
    //通过sc.broadcast 来创建一个广播变量。每一个Executor中会有该变量的一次Copy。一个Executor【JVM进程】中有很多分区
    val broadcastVar = sc.broadcast(abc)
    println(broadcastVar.value)   //通过value方法获取广播变量的内容。

    //使用方法
    val hashAcc = new CustomerAccumulator()
    sc.register(hashAcc, "abc")

    val rdd = sc.makeRDD(Array("a", "b", "c", "a", "b", "c", "d"),5)

    rdd.foreach(hashAcc.add)

    for ((k, v) <- hashAcc.value) {
      println(" [" + k + ":" + v + "]")
    }

    sc.stop()
  }
}
