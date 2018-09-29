package coreRDD

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

class CustomerAccumulatorII extends AccumulatorV2[String, String] {

  private var res = ""

  // 检测是否为空
  override def isZero: Boolean = {res == ""}

  // 合并每一个分区的输出 总sum
  override def merge(other: AccumulatorV2[String, String]): Unit =
    other match {
      case o: CustomerAccumulatorII => res += o.res
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

  // 拷贝一个新的累加器
  override def copy(): CustomerAccumulatorII = {
    val newMyAcc = new CustomerAccumulatorII
    newMyAcc.res = this.res
    newMyAcc
  }

  // 每一个分区中用于添加数据的方法  小SUM
  override def add(v: String): Unit = res += v + "-"

  // 重置一个累加器
  override def reset(): Unit = res = ""

  // 输出值
  override def value: String = res
}

object CustomerAccumulatorII {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Accumulator1").setMaster("local")
    val sc = new SparkContext(conf)

    val myAcc = new CustomerAccumulatorII
    sc.register(myAcc, "myAcc")

    //val acc = sc.longAccumulator("avg")
    val nums = Array("1", "2", "3", "4", "5", "6", "7", "8")
    val numsRdd = sc.parallelize(nums)

    numsRdd.foreach(num => myAcc.add(num))
    println(myAcc)
    sc.stop()
  }
}