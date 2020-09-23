package net.duxingzhe.luke.spark.scala.rdd

import net.duxingzhe.luke.spark.scala.rdd.util.MyAccumulator
import org.apache.spark.sql.SparkSession

object ScalaAccumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ScalaAccumulator")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    /*
    1.x版本的累加器，已经过时不推荐使用
     */
    // 定义累加器
    val accum = sc.accumulator(0, "My Accumulator")
    // Executor 上面只能对累加器进行累加操作，只有Driver才能读取累加器的值
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
    // 获取并打印累加器的值，Driver读取值的时候会把各个Executor上存储的本地累加器的值加起来
    println(accum.value)

    /*
    2.x版本的累加器
     */
    // Long类型和Double类型的累加器不用指定初始值，可以直接使用
    val accum2 = sc.longAccumulator("My Accumulator2")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum2.add(x))
    println(accum2.value)

    /*
    使用自定义累加器
     */
    val myAccum = new MyAccumulator
    // 向SparkContext注册累加器
    sc.register(myAccum)

    // 把 a, b, c, d 添加到累加器的result数组并打印出来
    sc.parallelize(Array("a", "b", "c", "d"))
      .foreach(x => myAccum.add(x))
    // 运行结果显示ArrayBuffer里的值的顺序不是固定的，取决于各个Executor的值到达Driver的顺序
    println(myAccum.value)


    sc.stop()
    spark.stop()
  }

}
