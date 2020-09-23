package net.duxingzhe.luke.spark.jss;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author luke yan
 * @Description Java Spark Sql DataSource Examples
 * @CreateDate 2020/09/11
 * @UpdateDate 2020/09/11
 */
public class JavaSqlDataSourceExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Sql DataSource")
                .master("local[*]")
                .getOrCreate();
        /*
        直接基于 parquet 文件进行SQL查询
         */
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`JavaSparkSql/src/main/resources/users.parquet`");
        sqlDF.show();

        /*
        将DataFrame保存为parquet格式
         */
        Dataset<Row> peopleDF = spark.read().json("JavaSparkSql/src/main/resources/people.json");
        // peopleDF 保存为parquet文件时，依然会保留着结构信息Schema
        peopleDF.write().parquet("JavaSparkSql/src/main/resources/people.parquet");

        // 读取上述创建的parquet文件
        // Parquet文件是自描述的，所以结构信息被保留
        //读取一个parquet文件的结果是一个已具有完整结构信息的DataFrame对象
        Dataset<Row> parquetFileDF = spark.read().parquet("JavaSparkSql/src/main/resources/people.parquet");
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
        namesDF.map((MapFunction<Row, String>) attr -> "Name: " + attr.apply(0), Encoders.STRING()).show();

spark.stop();
    }
}
