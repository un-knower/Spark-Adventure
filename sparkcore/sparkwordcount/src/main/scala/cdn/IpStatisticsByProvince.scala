package cdn

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

// 根据访问用户log日志中的 ip地址 计算出访问者的省份, ===> 计算每个省份的访问次数
object IpStatisticsByProvince {
  val IPPattern: Regex = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("IpStatisticsByProvince"))

    val log = sc.textFile("sparkcore\\sparkwordcount\\src\\main\\resources\\access.log").cache()
    val ipLocation = sc.textFile("sparkcore\\sparkwordcount\\src\\main\\resources\\ip.txt").cache()
    //20090121000132095572000|125.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/ EmbeddedWB- 14.59  from: http://bsalsa.com/ )|http://show.51.com/main.php|
    //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302

    def ipProvinceRange(): Array[(Long, Long, String)] = {
      val ipProvinceRange: Array[(Long, Long, String)] = ipLocation.map { x =>
        val para = x.split("[|]")
//      val para = x.split("\\|")
        (para(2).toLong, para(3).toLong, para(6))
      }.collect()

      ipProvinceRange
    }

    //ip转十进制方法
    def ip2Long(ip: String): Long = {
      val fragments = ip.split("[.]")
      var ipNum = 0L
      for (i <- 0 until fragments.length) {
        ipNum = fragments(i).toLong | ipNum << 8L
      }
      ipNum
    }

    //二分法
    def binarrySearch(rules: Array[(Long, Long, String)], ip: Long): Int = {
      var low = 0
      var high = rules.length - 1

      while (low <= high) {
        var middle = (low + high) / 2

        if ((ip >= rules(middle)._1) && ip <= rules(middle)._2) {
          return middle
        }

        if (ip < rules(middle)._1) {
          high = middle - 1
        } else {
          low = middle + 1
        }
      }
      -1
    }

    // 根据访问用户log日志中的 ip地址 计算出访问者的省份, ===> 计算每个省份的访问次数
    //    val ip = ip2Long("183.17.63.230")
    val ipProvinceArray = ipProvinceRange()

    //把 IP ProvinceRange Rule 规则广播
    val broadcastRule = sc.broadcast(ipProvinceArray)

    //定义方法 (广东省,12312)
    def function(line: String): (String, Int) = {
      val ip = IPPattern.findFirstIn(line).get
      //查找归属地
      //将ip转化为十进制
      val ipLong = ip2Long(ip)
      //获取广播变量的值
      val broadcastValue: Array[(Long, Long, String)] = broadcastRule.value

      val index = binarrySearch(broadcastValue, ipLong)

      var province = "未知"

      if (index != -1) {
        province = broadcastValue(index)._3
      }

      (province, 1)
    }

    val independentIP = log
      .map(function)
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    println(independentIP.collect().mkString("\n"))

    sc.stop()
  }
}