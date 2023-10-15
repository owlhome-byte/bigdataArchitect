package owl.com.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object lesson07_rdd_getTopN {
  def main(args: Array[String]): Unit = {
    val con = new SparkConf().setMaster("local").setAppName("rdd_over")
    val sc = new SparkContext(con)
    sc.setLogLevel("ERROR")

    // 排序规则
    implicit val test = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2)

    }

    // 获取同月份中 温度TopN的数据
    // 读取文件
    val data = sc.textFile("bigdataArchitect/sparkAll/data/tqdata")

    // 格式化数据源 年月日 温度
    val data_format = data.map(item => {
      val items = item.split("\\s+")
      val arrs = items(0).split("-")
      (arrs(0).toInt, arrs(1).toInt, arrs(2).toInt, items(1).toInt)
    })

    // 方式一：获取每月最高温的两天:  存在问题：原数据直接用groupby 容易OOM； 自定义函数中用到了toList.sorted也容易出现OOM
    println("------------------方式一：获取每月最高温的两天-----------------")
    val data_group = data_format.map(item => {
      ((item._1, item._2), (item._3, item._4))
    }).groupByKey()     // ((2019,6),CompactBuffer((1,39), (1,38), (2,31), (3,31)))
//    data_group.foreach(println)
    val res1 = data_group.mapValues(item => {
      // 通过新建一个hasMap用来去重
      val map: mutable.Map[Int, Int] = new mutable.HashMap()
      item.foreach(elem_kv => {
        if (map.get(elem_kv._1).getOrElse(0) < elem_kv._2) map.put(elem_kv._1, elem_kv._2)
      })
      // 排序（此处容易出现OOM）
      val tmp = map.toList.sorted(new Ordering[(Int, Int)] {
        override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2)
      })
      // 取值
      tmp.take(2)

    })
    res1.foreach(println)


    println("--------------------第二种：去重 分组 排序 取值")// 相对于一减轻了一定的数据量 但又groupBy  会有OOM的风险
    val data2_group = data_format.map(item => ((item._1, item._2, item._3), item._4)).reduceByKey((x,y) =>{if (x>y) x else y})  // 这个reduceByKey去重不错
    val data2_kv = data2_group.map(item => ((item._1._1, item._1._2), (item._1._3, item._2))).groupByKey()
    val res2 = data2_kv.mapValues(item => {
      // 排序
      item.toList.sorted(new Ordering[(Int, Int)] {
        override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2) // 排序器从大到小排
      }).take(2)
    })
    res2.foreach(println)

    println("-----------------------第三种：排序 分组 去重 取值")  //  优先排序减轻了代码量
    val data3_sort = data_format.sortBy(item => {
      (item._1, item._2, item._4)
    }, false)

    val data3_group = data3_sort.map(item => {
      ((item._1, item._2, item._3), item._4)
    }).reduceByKey((x, y) => if (x > y) x else y)
    val data3_group2 =  data3_group.map(item=>{((item._1._1,item._1._2),(item._1._3,item._2))}).groupByKey()
    data3_group2.mapValues(item=>{
      item.take(2)
    }).foreach(println)


    println("-------------------第四种：排序 combine -----------------")
    // map映射格式
    val data4 = data_format.map(item => {
      ((item._1, item._2), (item._3, item._4))
    })

    val res4 = data4 combineByKey(
      // 第一次的值
      (v1: (Int, Int)) => {Array(v1, (0, 0), (0, 0))},

      (oldv:Array[(Int, Int)], newv:(Int, Int)) => {
        var flag = 0;
        for (i <- 0 until oldv.length) {
          // 判断是否有相同的值，进行去重
          if (oldv(i)._1 == newv._1) {
            if (oldv(i)._2 < newv._2) {
              flag = 1
              oldv(i) = newv
            } else {
              flag = 2
            }
          }
        }
        if (flag == 0) {
          oldv(oldv.length - 1) = newv
        }
          // 排序
        oldv.sorted

      },

      (v1: Array[(Int, Int)], v2: Array[(Int, Int)]) => {
        // 去重
        val tmp = v1.union(v2)
        tmp.sorted
      }

    )
    res4.map(item=>(item._1,item._2.toList)).foreach(println)


    Thread.sleep(Long.MaxValue)
  }

}
