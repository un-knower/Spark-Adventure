package udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/*{"name":"Michael", "salary":3000}
{"name":"Andy", "salary":4500}
{"name":"Justin", "salary":3500}
{"name":"Berta", "salary":4000}
  目标：求平均工资  【工资的总额，工资的个数】*/
// =======================================================================
// 弱类型UDAF usually with group by (按 属性的相同字段group)
// =======================================================================
class AverageUDAF extends UserDefinedAggregateFunction{
  // 输入数据
  override def inputSchema: StructType = StructType( StructField("salary",LongType) :: Nil )

  // 每一个分区中的 共享变量
  override def bufferSchema: StructType = StructType( StructField("sum",LongType) :: StructField("count",IntegerType) :: Nil )

  // 表示UDAF的输出类型
  override def dataType: DataType = DoubleType

  // 表示如果有相同的输入是否会存在相同的输出，如果是则true
  override def deterministic: Boolean = true

  // 初始化每一个分区中的 共享变量
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0
  }

  // 每一个分区中的每一条数据聚合的时候需要调用该方法
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //获取这一行中的工资，然后将工资加入到sum中
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    //将工资的个数加1
    buffer(1) = buffer.getInt(1) + 1
  }

  // 将每一个分区的输出 合并 形成最后的数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 合并总的工资
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    // 合并总的工资个数
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  // 给出计算结果
  override def evaluate(buffer: Row): Any = {
    // 取出总的工资 / 总的工资个数
    buffer.getLong(0).toDouble / buffer.getInt(1)
  }

}

object AverageUDAF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf().setAppName("udaf").setMaster("local[*]")).getOrCreate()

    val employee = spark.read.json("sparksql\\sparksql_templ\\src\\main\\resources\\employees.json")

    employee.createOrReplaceTempView("employee")

    spark.udf.register("average",new AverageUDAF)

    spark.sql("select average(salary) from employee").show()

    spark.stop()
  }

}


