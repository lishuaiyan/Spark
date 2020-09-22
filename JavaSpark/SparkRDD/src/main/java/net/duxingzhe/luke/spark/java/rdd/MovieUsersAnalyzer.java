//package net.duxingzhe.luke.spark.java.rdd;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SparkSession;
//import scala.Tuple2;
//import scala.Tuple3;
//
///**
// * @Author luke yan
// * @Description
// * @CreateDate 2020/09/18
// * @UpdateDate 2020/09/18
// */
//public class MovieUsersAnalyzer {
//    public static void main(String[] args) {
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("RDD_Movies_Users_Analyzer")
//                .master("local[*]")
//                .getOrCreate();
//
//        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
//
//        // 设置Spark程序运行时日志级别为warn
//        sc.setLogLevel("warn");
//        String dataPath = "JavaSparkSql/src/main/resources/ml-1m";
//        JavaRDD<String> usersRdd = sc.textFile(dataPath + "users.dat");
//        JavaRDD<String> moviesRdd = sc.textFile(dataPath + "movies.dat");
//        JavaRDD<String> ratingsRdd = sc.textFile(dataPath + "ratings.dat");
//
//        JavaRDD<Tuple2<String, String>> movieInfo = moviesRdd.map(str -> str.split("::"))
//                .map(x -> new Tuple2<String, String>(x[0], x[1])).cache();
//        JavaRDD<Tuple3<String, String, String>> ratings = ratingsRdd.map(str -> str.split("::"))
//                .map(x -> new Tuple3<>(x[0], x[1], x[2])).cache();
//
//        ratings.map(x -> new Tuple2<String, Tuple2<Double, Integer>>(x._2(), new Tuple2<Double, Integer>(Double.valueOf(x._3()), 1)))
//                .reduce()
//
//
//
//
//        spark.stop();
//
//    }
//}
