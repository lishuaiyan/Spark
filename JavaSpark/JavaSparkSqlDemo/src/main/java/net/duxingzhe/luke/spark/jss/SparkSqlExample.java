package net.duxingzhe.luke.spark.jss;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
/**
 * @author luke yan
 * @Description Java Spark Sql Example
 * @CreateDate 2020/09/09
 * @UpdateDate 2020/09/09
 */
public class SparkSqlExample {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Example")
                .config("spark.some.config.option", "some-value")
                .master("local[*]")
                .getOrCreate();
        // 使用 SparkSession 提供的 read() 方法可读取数据源 (read方法返回一个DataFrameReader)，进而通过json()方法标识数据源具体类型为Json格式
        Dataset<Row> df = spark.read().json("JavaSparkSql/src/main/resources/people.json");

        //在返回的DataFrame对象使用show(n)方法，展示数据集前n条数据
        df.show(3);

        //1、以树格式输出DataFrame对象的结构信息 Schema
        df.printSchema();

        //2、通过DataFrame的select()方法查询数据集中name这一列
        df.select(col("name")).show(3);
        df.select("name").show(3);

        // 3、组合使用DataFrame对象的select()\where()\orderBy()方法查找身高大于175cm同学的姓名和下一学年的年龄一集体重情况并且使结果集内记录按照age字段进行
        //升序排列
        df.select(col("name"), col("age").plus(1), col("weight"))
                .where(col("height").gt(175))
                .orderBy(col("age").asc()).show(3);
        // 4、使用DataFrame对象提供的 groupBy 方法进而统计班级内学生年龄分布
        df.groupBy(col("age")).count().show();

        // 调用DataFrame提供的createOrReplaceTempView方法，将df (沿用记录着姓名、年龄身高、体重等学生信息的DataFrame对象)注册成临时表
        df.createOrReplaceTempView("student");
        // 调用 SparkSession提供的sql接口，对student临时表进行sql查询，需要注意的是sql()方法的返回值仍为DataFrame对象
        Dataset<Row> sqlDF = spark.sql("SELECT name, age FROM student");
        sqlDF.show(3);

        // 调用DataFrame提供的createGlobalTempView方法，将df注册成student全局，临时表，可在同一个Spark应用程序的多个session中共享
        df.createGlobalTempView("student");
        // 引用全局临时表时需用 global_temp 进行标识
        spark.sql("SELECT name, age, FROM global_temp.student").show();


        spark.stop();
    }
}
