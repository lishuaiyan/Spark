package net.duxingzhe.luke.spark.scala.rdd.moviesdemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MovieUsersAnalyzerRdd {
  def main(args: Array[String]): Unit = {
    // 设置打印日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[*]"

    var dataPath = "SparkRDD/src/main/resources/ml-1m/";

    if (args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }

    /*
    创建SparkContext
     */
    val sc = new SparkContext(
      new SparkConf().setMaster(masterUrl)
        .setAppName("MovieUsersAnalyzerRdd"))
    /*
    读取本地文件为RDD
     */
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    MovieUserAction(usersRDD, occupationsRDD, ratingsRDD)



    sc.stop()

  }

  def

  /**
   * 电影的用户行为分析
   * @param usersRDD
   * @param occupationsRDD
   * @param ratingsRDD
   */
  def MovieUserAction(usersRDD: RDD[String], occupationsRDD: RDD[String], ratingsRDD: RDD[String]): Unit = {

    val usersBasic: RDD[(String, (String, String, String))] = usersRDD.map(_.split("::"))
      .map(user => (
        // (OccupationID, (UserID, Gender, Age))
        user(3), (user(0), user(1), user(2))))

    for (elem <- usersBasic.collect().take(5)) {
      println("usersBasicRDD (OccupationID, (UserID, Gender, Age)): " + elem)
    }

    val occupations: RDD[(String, String)] = occupationsRDD.map(_.split("::"))
      .map(job => (
        //(OccupationID, OccupationName)
        job(0), job(1)))

    for (elem <- occupations.collect().take(5)) {
      println("occupationsRDD (OccupationID, OccupationName): " + elem)
    }

    val userInformation: RDD[(String, ((String, String, String), String))] = {
      // (OccupationID, (UserID, gender, Age), OccupationName)
      usersBasic.join(occupations)
    }
    userInformation.cache()

    for (elem <- userInformation.collect().take(5)) {
      println("userInformationRDD (OccupationID, (UserID, gender, Age), OccupationName): " + elem)
    }

    val targetMovie: RDD[(String, String)] = ratingsRDD.map(_.split("::"))
      .map(x => (
        // (UserID, MovieID)
        x(0), x(1)))
      .filter(_._2.equals("1193"))

    for (elem <- targetMovie.collect().take(10)) {
      println("targetMovie (UserID, MovieID): " + elem)
    }

    val targetUsers = userInformation.map(x =>
      // (UserID, ((UserID, Gender, Age), OccupationName))
      (x._2._1._1, x._2))

    for (elem <- targetUsers.collect().take(5)) {
      println("targetUsers (UserID, ((UserID, Gender, Age), OccupationName)): " + elem)
    }

    val userInformationForSpecificMovie: RDD[(String, (String, ((String, String, String), String)))] = {
      // (UserID, (MovieID, ((UserID, Gender, Age), OccupationName))
      targetMovie.join(targetUsers)
    }


  }

}
