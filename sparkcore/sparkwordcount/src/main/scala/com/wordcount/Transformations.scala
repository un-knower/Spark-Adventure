package com.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object Transformations extends App {
  val sc = new SparkContext(new SparkConf().setAppName("wordCount").setMaster("local[*]"))

  //因为我是十二核电脑 ,默认是电脑核数和2的max
  val rdd = sc.makeRDD(0 to 10)
  println("====== 1、def map[U: ClassTag](f: T => U): RDD[U]  一对一转换=================================")
  println(rdd.map(x => x * 2).collect.mkString(" "))
  //Array[Int] = Array(0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20)

  println("====== 2、def filter(f: T => Boolean): RDD[T]   传入一个Boolean的方法，过滤数据=================================")
  println(rdd.filter(_ % 3 == 0).collect.mkString(" "))
  //Array[Int] = Array(0, 3, 6, 9)

  println("====== 3、def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]  一对多，并将多压平=================================")
  println(rdd.map(1 to _).collect.mkString(" "))
  //Array[scala.collection.immutable.Range.Inclusive] = Array(Range(), Range(1), Range(1, 2), Range(1, 2, 3), Range(1, 2, 3, 4), Range(1, 2, 3, 4, 5), Range(1, 2, 3, 4, 5, 6), Range(1, 2, 3, 4, 5, 6, 7), Range(1, 2, 3, 4, 5, 6, 7, 8), Range(1, 2, 3, 4, 5, 6, 7, 8, 9), Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  println(rdd.flatMap(1 to _).collect.mkString(" "))
  //Array[Int] = Array(1, 1, 2, 1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

  println("====== 4、def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], p: Boolean = false): RDD[U]  对于一个分区中的所有数据执行一个函数，性能比map要高=================================")
  println(rdd.mapPartitions(itemsRDD => itemsRDD.filter(_ % 3 == 0).map(_ + "hello")).collect.mkString(" "))
  //Array[String] = Array(0hello, 3hello, 6hello, 9hello)

  println("====== 5、def mapPartitionsWithIndex[U: ClassTag](f: (Int, Iterator[T]) => Iterator[U], p: Boolean = false): RDD[U]=================================")
  val rdd5 = sc.makeRDD(1 to 10, 5)
  println(rdd5.partitions.size) //5
  println(rdd5.mapPartitionsWithIndex((pNum, items) => Iterator(pNum + ": [" + items.mkString(",") + "]")).collect.mkString(" "))
  //Array[String] = Array(0: [1,2], 1: [3,4], 2: [5,6], 3: [7,8], 4: [9,10])

  println("====== 6、def sample(withReplacement: Boolean,fraction: Double,seed: Long = Utils.random.nextLong): RDD[T]  主要用于抽样=================================")
  println(rdd5.sample(true, 0.3, 4).collect.mkString(" "))
  //Array[Int] = Array(2 3 4 5 5)

  println("====== 7、def union(other: RDD[T]): RDD[T]  联合一个RDD，返回组合的RDD=================================")
  println(sc.makeRDD(1 to 5).union(sc.makeRDD(5 to 10)).collect.mkString(" "))
  //Array[Int] = Array(1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10)

  println("====== 8、def intersection(other: RDD[T]): RDD[T]  求交集=================================")
  println(sc.makeRDD(1 to 5).intersection(sc.makeRDD(5 to 10)).collect.mkString(" "))
  //Array[Int] = Array(5)

  println("====== 9、def distinct(): RDD[T]  去重=================================")
  println(sc.makeRDD(1 to 5).union(sc.makeRDD(5 to 10)).distinct.collect.mkString(" "))
  //Array[Int] = Array(4, 8, 1, 9, 5, 6, 10, 2, 3, 7)

  println("====== 10、def partitionBy(partitioner: Partitioner): RDD[(K, V)]  用提供的分区器分区\t\tHashPartitioner就是求余=================================")
  val rdd10 = sc.makeRDD(1 to 10, 5)
  println(rdd10.map(x => (x, " ")).partitionBy(new org.apache.spark.HashPartitioner(5)).mapPartitionsWithIndex((i, items) => Iterator(i + ": [" + items.mkString(",") + "]")).collect.mkString(" "))
  //Array[String] = Array(0: [(5, ),(10, )], 1: [(1, ),(6, )], 2: [(2, ),(7, )], 3: [(3, ),(8, )], 4: [(4, ),(9, )])

  println("====== 11、def reduceByKey(func: (V, V) => V): RDD[(K, V)]  根据Key进行聚合  预聚合。=================================")
  val rdd11 = sc.makeRDD(Array((1, 1), (1, 2), (2, 3), (2, 4), (3, 5)))
  println(rdd11.reduceByKey(_ + _).collect.mkString(" "))
  //Array[(Int, Int)] = Array((2,7), (1,3), (3,5))

  println("====== 12、def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]  将key相同的value聚集在一起。=================================")
  val rdd12 = sc.makeRDD(Array((1, 2), (1, 1), (2, 4), (2, 3), (3, 5)))
  println(rdd12.groupByKey.collect.mkString(" "))
  //Array[(Int, Iterable[Int])] = Array((1,CompactBuffer(2, 1)) (2,CompactBuffer(4, 3)) (3,CompactBuffer(5)))

  println(rdd12.groupByKey.map(t => (t._1, t._2.sum)).collect.mkString(" "))
  //Array[(Int, Int)] = Array((2,7), (1,3), (3,5))

  /*13、def combineByKey[C](
  createCombiner: V => C,		会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就 和之前的某个元素的键相同。如果这是一个新的元素,combineByKey() 会使用一个叫作 createCombiner() 的函数来创建
    那个键对应的累加器的初始值
  mergeValue: (C, V) => C,	如果这是一个在处理当前分区之前已经遇到的键， 它会使用 mergeValue() 方法将该键的累加器对应的当前值与这个新的值进行合并
  mergeCombiners: (C, C) => C,	mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并
  ): RDD[(K, C)]*/
  println("====== 13、def combineByKey[C](createCombiner: V => C,mergeValue: (C, V) => C,mergeCombiners: (C, C) => C): RDD[(K, C)]=================================")
  val rdd13 = sc.makeRDD(Array(("a", 90), ("a", 80), ("a", 60), ("b", 78), ("b", 84), ("b", 96), ("c", 90), ("c", 86)))
  //求每个Key的平均数
  println(rdd13.combineByKey(
    value => (value, 1), //V => C
    (t: (Int, Int), value) => (t._1 + value, t._2 + 1), //(C, V) => C
    (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2) //(C, C) => C
  ).map { case (k, v) => (k, v._1 / v._2) }.collect.mkString(" ")) //这里推荐用 case模式匹配匹配(k,v)结构
  //  Array[(String, Int)] = Array((b,86), (a,76), (c,88))
  //.map(t => (t._1, t._2._1 / t._2._2)).collect //map函数是传一个T 变成一个U	不好读不推荐


  /*14、def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]  是CombineByKey的简化版，可以通过zeroValue直接提供一个初始值。
    seqOp: (U, V) => U 在单个分区内对相同Key操作
    combOp: (U, U) => U 对多个分区的对相同Key的U结果操作*/
  println("====== 14、def aggregateByKey[U: ClassTag](zeroValue: U, partition: Partitioner)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)] =================================")
  //求各个区最大值相加的值
  val rdd141 = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 1)
  println(rdd141.aggregateByKey(0)(math.max(_, _), _ + _).collect.mkString(" "))
  //  Array[(Int, Int)] = Array((1,4), (3,8), (2,3))
  val rdd14 = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
  println(rdd14.aggregateByKey(0)(math.max(_, _), _ + _).collect.mkString(" "))
  //  Array[(Int, Int)] = Array((3,8), (1,7), (2,3))

  println(rdd14.combineByKey(
    value => math.max(0, value),
    (c: Int, v) => math.max(c, v),
    (c1: Int, c2: Int) => c1 + c2
  ).collect.mkString(" ")) //与 aggregateByKey相同, aggregateByKey是CombineByKey的简化版
  //  Array[(Int, Int)] = Array((3,8), (1,7), (2,3))

  //求每个key最大值
  println(rdd14.aggregateByKey(0)(math.max(_, _), math.max(_, _)).collect.mkString(" "))
  //  Array[(Int, Int)] = Array((3,8), (1,4), (2,3))

  println(rdd14.combineByKey(
    value => math.max(0, value),
    (c: Int, v) => math.max(c, v),
    (c1: Int, c2: Int) => math.max(c1, c2)
  ).collect.mkString(" "))
  //  Array[(Int, Int)] = Array((3,8), (1,4), (2,3))

  println("====== 15、def foldByKey(zeroValue: V, partition: Partitioner)(f: (V, V) => V): RDD[(K, V)]  该函数为aggregateByKey的简化版，seqOp和combOp一样，相同。 =================================")
  //求每个key最大值
  val rdd15 = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
  println(rdd15.foldByKey(0)(math.max(_, _)).collect.mkString(" "))
  //  Array[(Int, Int)] = Array((3,8), (1,4), (2,3))

  println("====== 16、def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length) : RDD[(K, V)]  根据Key来进行排序，如果Key目前不支持排序，需要with Ordering接口，实现compare方法，告诉spark key的大小判定。 =================================")
  val rdd16 = sc.parallelize(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
  rdd16.sortByKey().collect()
  // Array[(Int, String)] = Array((1,dd), (2,bb), (3,aa), (6,cc))
  rdd16.sortByKey(false).collect()
  // Array[(Int, String)] = Array((6,cc), (3,aa), (2,bb), (1,dd))

  println("====== 17、def sortBy[K]( f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length) (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]   根据f函数提供可以排序的key =================================")
  val rdd17 = sc.parallelize(List(4, 2, 3, 1))
  rdd17.sortBy(x => x).collect()
  //  Array[Int] = Array(1, 2, 3, 4)
  rdd17.sortBy(x => x % 3).collect()
  //  Array[Int] = Array(3, 1, 4, 2)

  println("====== 18、def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]  连接两个RDD的数据。 =================================")
  /*JOIN ：  只留下双方都有KEY
  left JOIN：  留下左边RDD所有的数据
  right JOIN： 留下右边RDD所有的数据*/
  val rddL = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
  val rddR = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))
  println(rddL.join(rddR).collect().mkString(", "))
  //  Array[(Int, (String, Int))] = Array((1,(a,4)), (2,(b,5)), (3,(c,6)))
  val rddr = sc.parallelize(Array((1, 4), (2, 5), (3, 6), (4, 4))) //(4,4) rddL没有
  println(rddL.rightOuterJoin(rddr).collect.mkString(", "))
  //  Array[(Int, (Option[String], Int))] = Array((1,(Some(a),4)), (2,(Some(b),5)), (3,(Some(c),6)), (4,(None,4)))

  println("====== 19、def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner) : RDD[(K, (Iterable[V], Iterable[W]))]  分别将相同key的数据聚集在一起。 =================================")
  val rdd1 = sc.makeRDD(Array((1, 1), (1, "e"), (2, 3), (2, 4), (3, 5)))
  val rdd2 = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))
  println(rdd1.cogroup(rdd2).collect.mkString(","))
  //  Array[(Int, (Iterable[Int], Iterable[String]))] = Array((1,(CompactBuffer(1, e),CompactBuffer(a, d))),(2,(CompactBuffer(3, 4),CompactBuffer(b))),(3,(CompactBuffer(5),CompactBuffer(c))))

  /*20、def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)]  做笛卡尔积。  n * m
  21、def pipe(command: String): RDD[String] 执行外部脚本
  val rdd = sc.parallelize(List("hi","Hello","how","are","you"),1)
  rdd.pipe("/home/atguigu/pipe.sh").collect()
  res38: Array[String] = Array(AA, >>>hi, >>>Hello, >>>how, >>>are, >>>you)*/

  println("====== 22、def coalesce(numPartitions: Int, shuffle: Boolean = false,partition: Option[Partition] = Option.empty)(implicit ord: Ordering[T] = null): RDD[T]   缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。 =================================")
  val rdd22 = sc.parallelize(1 to 16, 4)
  val coalesceRDD = rdd22.coalesce(3)
  println(coalesceRDD.partitions.size)
  //: Int = 3
  rdd22.mapPartitionsWithIndex((i, items) => Iterator(i + ": [" + items.mkString(",") + "]")).collect
  // Array[String] = Array(0: [1,2,3,4], 1: [5,6,7,8], 2: [9,10,11,12], 3: [13,14,15,16])
  coalesceRDD.mapPartitionsWithIndex((i, items) => Iterator(i + ": [" + items.mkString(",") + "]")).collect
  // Array[String] = Array(0: [1,2,3,4], 1: [5,6,7,8], 2: [9,10,11,12,13,14,15,16])

  println("====== 23、def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] 重新分区。 =================================")
  val rer23 = rdd22.repartition(4)
  rer23.mapPartitionsWithIndex((i, items) => Iterator(i + ": [" + items.mkString(",") + "]")).collect
  // Array[String] = Array(0: [2,6,10,14], 1: [3,7,11,15], 2: [4,8,12,16], 3: [1,5,9,13])

  println("====== 24、def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)]  如果重新分区后需要排序，那么直接用这个。在给定的partitioner内部进行排序 =================================")
  val rdd24 = sc.parallelize(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
  rdd24.mapPartitionsWithIndex((i, items) => Iterator(i + ": [" + items.mkString(",") + "]")).collect
  //  Array[String] = Array(0: [(3,aa),(6,cc)], 1: [(2,bb),(1,dd)])

  val rddnew = rdd24.repartitionAndSortWithinPartitions(new org.apache.spark.HashPartitioner(2))
  rddnew.mapPartitionsWithIndex((i, items) => Iterator(i + ": [" + items.mkString(",") + "]")).collect
  //  Array[String] = Array(0: [(2,bb),(6,cc)], 1: [(1,dd),(3,aa)])

  println("====== 25、def glom(): RDD[Array[T]] 将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]] =================================")
  val rdd25 = sc.parallelize(1 to 16, 4)
  rdd25.collect
  //  Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
  rdd25.glom().collect()
  //  Array[Array[Int]] = Array(Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(9, 10, 11, 12), Array(13, 14, 15, 16))

  println("====== 26、def mapValues[U](f: V => U): RDD[(K, U)] 对于KV结构RDD，只处理value =================================")
  val rdd26 = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))
  rdd26.mapValues(_ + "|||").collect()
  //  Array[(Int, String)] = Array((1,a|||), (1,d|||), (2,b|||), (3,c|||))
  rdd26.flatMapValues(_ + "|||").collect()
  //  Array[(Int, Char)] = Array((1,a), (1,|), (1,|), (1,|), (1,d), (1,|), (1,|), (1,|), (2,b), (2,|), (2,|), (2,|), (3,c), (3,|), (3,|), (3,|))

  println("======  27、def subtract(other: RDD[T]): RDD[T]  去掉和other重复的元素 =================================")
  val rdd27 = sc.parallelize(3 to 8)
  val rdd28 = sc.parallelize(1 to 5)
  rdd27.subtract(rdd28).collect()
  //  Array[Int] = Array(6, 8, 7)	//3 4 5 在rdd1中有

  sc.stop()
}
