//package net.duxingzhe.luke.spark.java.rdd.moviesdemo;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//import scala.Tuple3;
//
///**
// * @author luke yan
// * @Description
// * @CreateDate 2020/09/25
// * @UpdateDate 2020/09/25
// */
//public class MovieUsersAnalyzerRdd {
//    public static void main(String[] args) {
//        Logger.getLogger("org").setLevel(Level.ERROR);
//
//        String masterUrl = "local[*]";
//        String dataPath = "SparkRDD/src/main/resources/ml-1m/";
//
//        if (args.length > 0) {
//            masterUrl = args[0];
//        } else if (args.length > 1) {
//            dataPath = args[1];
//        }
//
//        /*
//        创建SparkContext
//         */
//        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster(masterUrl)
//        .setAppName("MovieUsersAnalyzerRdd"));
//
//        /*
//        读取本地文件为RDD
//         */
//        JavaRDD<String> usersRdd = sc.textFile(dataPath + "users.dat");
//        JavaRDD<String> moviesRdd = sc.textFile(dataPath + "movies.dat");
//        JavaRDD<String> occupationsRdd = sc.textFile(dataPath + "occupations.dat");
//        JavaRDD<String> ratingsRdd = sc.textFile(dataPath + "ratings.dat");
//
//        JavaPairRDD<String, Tuple3<String, String, String>> usersBasic = usersRdd.map(line -> line.split("::"))
//                // ()
//                .mapToPair(user -> new Tuple2<>(user[3], new Tuple3<>(user[0], user[1], user[2]));
//    }
//}
