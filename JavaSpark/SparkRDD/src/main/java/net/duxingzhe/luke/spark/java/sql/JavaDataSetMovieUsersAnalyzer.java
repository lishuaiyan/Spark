package net.duxingzhe.luke.spark.java.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import java.io.Serializable;

/**
 * @author luke yan
 * @Description
 * @CreateDate 2020/09/23
 * @UpdateDate 2020/09/23
 */
public class JavaDataSetMovieUsersAnalyzer {
    public static class User implements Serializable {
        private String userId;
        private String gender;
        private String age;
        private String occupationId;
        private String zipCode;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        public String getOccupationId() {
            return occupationId;
        }

        public void setOccupationId(String occupationId) {
            this.occupationId = occupationId;
        }

        public String getZipCode() {
            return zipCode;
        }

        public void setZipCode(String zipCode) {
            this.zipCode = zipCode;
        }
    }
    public static class Rating implements Serializable {
        private String userId;
        private String movieId;
        private Double rating;
        private String timeStamp;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getMovieId() {
            return movieId;
        }

        public void setMovieId(String movieId) {
            this.movieId = movieId;
        }

        public Double getRating() {
            return rating;
        }

        public void setRating(Double rating) {
            this.rating = rating;
        }

        public String getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(String timeStamp) {
            this.timeStamp = timeStamp;
        }
    }
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaDataSetMovieUsersAnalyzer")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // 设置Spark程序运行时日志级别为warn
        sc.setLogLevel("warn");
        String dataPath = "SparkRDD/src/main/resources/ml-1m/";
        JavaRDD<String> usersRdd = sc.textFile(dataPath + "users.dat");
        JavaRDD<String> moviesRdd = sc.textFile(dataPath + "movies.dat");
        JavaRDD<String> ratingsRdd = sc.textFile(dataPath + "ratings.dat");

        JavaRDD<User> usersForDsRdd = usersRdd.map(line -> {
            String[] arr = line.split("::");
            User user = new User();
            user.setUserId(arr[0].trim());
            user.setGender(arr[1].trim());
            user.setAge(arr[2].trim());
            user.setOccupationId(arr[3].trim());
            user.setZipCode(arr[4].trim());
            return user;
        });
        // javaRDD 转 Dataset
        Dataset<User> usersDataSet = spark.createDataFrame(usersForDsRdd, User.class).as(Encoders.bean(User.class));
        usersDataSet.show(10);

        JavaRDD<Rating> ratingsForDsRdd = ratingsRdd.map(line -> {
            String[] arr = line.split("::");
            Rating rating = new Rating();
            rating.setUserId(arr[0].trim());
            rating.setMovieId(arr[1].trim());
            rating.setRating(Double.valueOf(arr[2].trim()));
            rating.setTimeStamp(arr[3].trim());
            return rating;
        });
        Dataset<Rating> ratingsDataSet = spark.createDataFrame(ratingsForDsRdd, Rating.class).as(Encoders.bean(Rating.class));

        ratingsDataSet.filter("movieId = 1193")
                .join(usersDataSet, "userId")
                .select("gender", "age")
                .groupBy("gender", "age")
                .count()
                .orderBy(expr("gender").desc(), expr("age"))
                .show();

    }
}
