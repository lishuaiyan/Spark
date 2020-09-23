package net.duxingzhe.luke.spark.java.rdd;

import net.duxingzhe.luke.spark.java.rdd.util.SecondSortKey;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

/**
 * @Author luke yan
 * @Description
 * @CreateDate 2020/09/18
 * @UpdateDate 2020/09/18
 */
public class JavaRddMovieUsersAnalyzer {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaRddMovieUsersAnalyzer")
                .master("local[*]")
                /*
                getOrCreate方法检测当前线程是否有一个已经存在的ThreadLocal级别的SparkSession，如果有则返回它
                如果没有，则检测是否有全局级别的SparkSession，有则返回，没有则创建新的SparkSession
                 */
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // 设置Spark程序运行时日志级别为warn
        sc.setLogLevel("warn");
        String dataPath = "SparkRDD/src/main/resources/ml-1m/";
        JavaRDD<String> usersRdd = sc.textFile(dataPath + "users.dat");
        JavaRDD<String> moviesRdd = sc.textFile(dataPath + "movies.dat");
        JavaRDD<String> ratingsRdd = sc.textFile(dataPath + "ratings.dat");

        JavaPairRDD<String, String> movieInfo = moviesRdd.map(str -> str.split("::"))
                .mapToPair(x -> new Tuple2<String, String>(x[0], x[1])).cache();
        JavaRDD<Tuple3<String, String, String>> ratings = ratingsRdd.map(str -> str.split("::"))
                .map(x -> new Tuple3<>(x[0], x[1], x[2])).cache();

        JavaPairRDD<String, Tuple2<Double, Integer>> moviesAndRatings = ratings.mapToPair(x ->
                new Tuple2<String, Tuple2<Double, Integer>>(x._2(), new Tuple2<Double, Integer>(Double.valueOf(x._3()), 1)))
                .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));
        // 得到 包含电影ID 和 平均分的 JavaPairRDD
        JavaPairRDD<String, Double> avgRatings = moviesAndRatings.mapToPair(x -> new Tuple2<>(x._1, x._2._1 / x._2._2));

        avgRatings.join(movieInfo).mapToPair(x -> new Tuple2<>(x._2._1, x._2._2))
                .sortByKey(false)
                .take(10)
                .forEach( record -> System.out.println(record._2 + "评分为： " + record._1 ));

        /*
        分析最受男性喜爱的电影Top10和最受女性喜爱的电影Top10
         */
        JavaPairRDD<String, String> usersGender = usersRdd.map(line -> line.split("::"))
                .mapToPair(x -> new Tuple2<>(x[0], x[1]));
        JavaPairRDD<String, Tuple2<Tuple3<String, String, String>, String>> genderRatings = ratings.mapToPair(x -> new Tuple2<>(x._1(), new Tuple3<>(x._1(), x._2(), x._3())))
                .join(usersGender).cache();
        genderRatings.take(10).forEach(System.out::println);
        // 分别过滤出男性和女性的记录
        JavaRDD<Tuple3<String, String, String>> maleFilteredRatings = genderRatings
                .filter(x -> "M".equals(x._2()._2()))
                .map(x -> x._2()._1());
        JavaRDD<Tuple3<String, String, String>> femaleFilteredRatings = genderRatings
                .filter(x -> "F".equals(x._2()._2()))
                .map(x -> x._2()._1());
        // 最受男性喜爱的电影Top10
        maleFilteredRatings.mapToPair(x -> new Tuple2<>(x._2(), new Tuple2<>(Double.valueOf(x._3()), 1)))
                .reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()))
                .mapToPair(x -> new Tuple2<>(x._1(), x._2()._1() / x._2()._2()))
                .join(movieInfo)
                .mapToPair(x -> new Tuple2<>(x._2()._1(), x._2()._2()))
                .sortByKey(false)
                .take(10)
                .forEach(record -> System.out.println(record._2() + "评分为：" + record._1()));

        // 最受女性喜爱的电影Top10
        femaleFilteredRatings.mapToPair(x -> new Tuple2<>(x._2(), new Tuple2<>(Double.valueOf(x._3()), 1)))
                .reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()))
                .mapToPair(x -> new Tuple2<>(x._1(), x._2()._1() / x._2()._2()))
                .join(movieInfo)
                .mapToPair(x -> new Tuple2<>(x._2()._1(), x._2()._2()))
                .sortByKey(false)
                .take(10)
                .forEach(record -> System.out.println(record._2() + "评分为：" + record._1()));

        //自定义实现二次排序
        JavaPairRDD<SecondSortKey, String> pairWithSortKey = ratingsRdd
                .mapToPair( line -> {
                    String[] splited = line.split("::");
                    return new Tuple2<>(new SecondSortKey(Double.valueOf(splited[3]), Double.valueOf(splited[2])), line);
                });
        JavaPairRDD<SecondSortKey, String> sorted = pairWithSortKey.sortByKey(false);

        JavaRDD<String> sortedResult = sorted.map(Tuple2::_2);

        sortedResult.take(10).forEach(System.out::println);

        // 关闭sparkContext
        sc.stop();
        // 关闭 SparkSession
        spark.stop();

    }
}
