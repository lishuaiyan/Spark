package net.duxingzhe.luke.spark.scala.sql

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ScalaDFMovieUsersAnalyzer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ScalaDFMovieUsersAnalyzer")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = "SparkRDD/src/main/resources/ml-1m/"
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    /*
    实现某部电影观看者中男性和女性不同年龄人数
     */
    val schemaForUsers = StructType(
      "UserID::Gender::Age::OccupationID::Zip-code".split("::")
        .map(column => StructField(column, StringType, nullable = true))
    )

    val usersRDDRows = usersRDD
      .map(_.split("::"))
      .map(line =>
        Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    val usersDF = spark.createDataFrame(usersRDDRows, schemaForUsers)

    val schemaForRatings = StructType(
      "UserID::MovieID".split("::")
        .map(column => StructField(column, StringType, nullable = true))
    ).add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)

    val ratingsRDDRows = ratingsRDD
      .map(_.split("::"))
      .map(line =>
        Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDF = spark.createDataFrame(ratingsRDDRows, schemaForRatings)

    val schemaForMovies = StructType(
      "MovieID::Title::Genres".split("::")
        .map(column => StructField(column, StringType, true))
    )
    val moviesRDDRows = moviesRDD
      .map(_.split("::"))
      .map(line =>
        Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim))
    val moviesDF = spark.createDataFrame(moviesRDDRows, schemaForMovies)

    //    ratingsDF.filter(s"MovieID = 1193")
    //      .join(usersDF, "UserID")
    //      .select("Gender", "Age")
    //      .groupBy("Gender", "Age")
    //      .count()
    //      .show()


    /*
    用LocalTempView实现某部电影观看者中不同性别不同年龄分别有所少人
     */

    ratingsDF.createTempView("ratings")
    usersDF.createTempView("users")

    val sql =
      s"""
         |SELECT
         | Gender
         |,Age
         |,count(*)
         | FROM
         | users u join ratings as r on u.UserID = r.UserID
         | where MovieID = 1193
         | group by Gender, Age
         |""".stripMargin
    spark.sql(sql).show()


    import spark.implicits._
    ratingsDF.select(col("MovieID"), col("Rating"))
      .groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc).show(10)

    /*
    DataFrame 和 RDD 混合编程
     */

    ratingsDF.select("MovieID", "Rating")
      .groupBy("MovieID")
      .avg("Rating")
      .rdd.map(row => (row(1), (row(0), row(1))))
      .sortBy(_._1.toString.toDouble, false)
      .map(tuple => tuple._2)
      .collect().take(10).foreach(println)
    spark.stop()
  }

}
