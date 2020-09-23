package net.duxingzhe.luke.spark.java.sql;

import javafx.scene.control.cell.MapValueFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import static org.apache.spark.sql.functions.*;

/**
 * @author luke yan
 * @Description
 * @CreateDate 2020/09/23
 * @UpdateDate 2020/09/23
 */
public class JavaDataFrameMovieUserAnalyzer {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaDataFrameMovieUserAnalyzer")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // 设置Spark程序运行时日志级别为warn
        sc.setLogLevel("warn");
        String dataPath = "SparkRDD/src/main/resources/ml-1m/";
        JavaRDD<String> usersRdd = sc.textFile(dataPath + "users.dat");
        JavaRDD<String> moviesRdd = sc.textFile(dataPath + "movies.dat");
        JavaRDD<String> ratingsRdd = sc.textFile(dataPath + "ratings.dat");

        String schemaUsers = "UserID::Gender::Age::OccupationID::Zip-code";
        List<StructField> fields = new ArrayList<>(5);
        for (String fieldName : schemaUsers.split("::")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schemaForUsers = DataTypes.createStructType(fields);

        JavaRDD<Row> usersRDDRows = usersRdd
                .map(x -> {
                    String[] arr = x.split("::");
                    return RowFactory.create(
                            arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), arr[4].trim()
                    );
                });
        Dataset<Row> usersDataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers);

        String schemaRatings = "UserID::MovieID";
        List<StructField> fieldsRatings = new ArrayList<>(2);
        for (String fieldName : schemaRatings.split("::")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fieldsRatings.add(field);
        }
        StructType schemaForRatings = DataTypes.createStructType(fieldsRatings)
                .add("Rating", DataTypes.DoubleType, true)
                .add("Timestamp", DataTypes.StringType, true);
        JavaRDD<Row> ratingsRDDRows = ratingsRdd
                .map(line -> {
                    String[] arr = line.split("::");
                    return RowFactory.create(
                            arr[0].trim(), arr[1].trim(), Double.valueOf(arr[2].trim()), arr[3].trim()
                    );
                });
        Dataset<Row> ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaForRatings);

        String schemaMovies = "MovieID::Title::Genres";
        List<StructField> fieldsMovies = new ArrayList<>(3);
        for (String fieldsName : schemaMovies.split("::")) {
            StructField field = DataTypes.createStructField(fieldsName, DataTypes.StringType, true);
            fieldsMovies.add(field);
        }
        StructType schemaForMovies = DataTypes.createStructType(fieldsMovies);

        JavaRDD<Row> moviesRDDRows = moviesRdd
                .map(line -> {
                    String[] arr = line.split("::");
                    return RowFactory.create(
                            arr[0].trim(), arr[1].trim(), arr[2].trim()
                    );
                });
        Dataset<Row> moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaForMovies);

        ratingsDataFrame.printSchema();

        ratingsDataFrame.filter("MovieID = 1193")
                .join(usersDataFrame, "UserID")
                .select("Gender", "Age")
                .groupBy("Gender", "Age")
                .count()
                .show(10);

        /*
        用LocalTempView实现某部电影观看者中不同性别不同年龄分别有多少人
         */
        ratingsDataFrame.createTempView("ratings");
        usersDataFrame.createTempView("users");

        String sql = "select" +
                " Gender" +
                ",Age" +
                ",count(*)" +
                " from users as u" +
                " join ratings as r" +
                " on u.UserID = r.UserID where MovieID = 1193 group by Gender, Age";
        spark.sql(sql).show(10);

        ratingsDataFrame.select("MovieID", "Rating")
                .groupBy("MovieID")
                .avg("Rating")
                .orderBy(col("avg(Rating)").desc())
                .show(10);

        /*
        DataFrame 和 RDD 混合编程
         */
        ratingsDataFrame.select("MovieID", "Rating")
                .groupBy("MovieID")
                .avg("Rating")
                .javaRDD()
                .mapToPair(row -> new Tuple2<>(Double.valueOf(row.get(1).toString()), new Tuple2<>(row.get(0), row.get(1))))
                .sortByKey(false)
                .map(Tuple2::_2)
                .collect()
                .forEach(System.out::println);




    }
}
