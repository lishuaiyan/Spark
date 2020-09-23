package net.duxingzhe.luke.spark.scala.rdd

import net.duxingzhe.luke.spark.scala.rdd.util.SecondarySortKey
import org.apache.spark.sql.SparkSession

object ScalaRddMovieUsersAnalyzer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ScalaRddMovieUsersAnalyzer")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
    val dataPath = "SparkRDD/src/main/resources/ml-1m/"
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    val movieInfo = moviesRDD.map(_.split("::"))
      .map(x => (x(0), x(1))).cache()
    val ratings = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1), x(2))).cache()
    val moviesAndRatings = ratings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val avgRatings = moviesAndRatings.map(x => (x._1, x._2._1.toDouble / x._2._2))
    avgRatings.join(movieInfo).map(x => (x._2._1, x._2._2))
      .sortByKey(ascending = false).take(10)
      .foreach(record => println(record._2 + " 评分为：" + record._1))

    /*
    最受男性和最受女性喜爱的电影Top10
     */
    val usersGender = usersRDD.map(_.split("::")).map(x => (x(0), x(1)))
    val genderRatings = ratings.map(x => (x._1, (x._1, x._2, x._3)))
      .join(usersGender).cache()

    val maleFilteredRatings = genderRatings.filter(x => x._2._2.equals("M"))
      .map(x => (x._2._1))
    val femaleFilteredRatings = genderRatings.filter(x => x._2._2.equals("F"))
      .map(x => x._2._1)
    maleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
      .join(movieInfo)
      .map(x=> (x._2._1, x._2._2))
      .sortByKey(ascending = false)
      .take(10)
      .foreach(record => println(record._2 + " 评分为 " + record._1))

    femaleFilteredRatings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1.toDouble / x._2._2))
      .join(movieInfo)
      .map(x => (x._2._1, x._2._2))
      .sortByKey(ascending = false)
      .take(10)
      .foreach(record => println(record._2 + "评分为 " + record._1))

    /*
    电影评分数据以Timestamp 和 Ratings 两个维度进行二次降序排列
     */
    val pairWithSortKey = ratingsRDD.map(line => {
      val splited = line.split("::")
      (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line)
    })
    // 直接调用sortBykey，此时会按照之前实现的compare方法排序
    val sorted = pairWithSortKey.sortByKey(ascending = false)
    val sortedResult = sorted.map(sortedline => sortedline._2 )
    sortedResult.take(10).foreach(println)


    spark.stop()
  }
}
