package net.duxingzhe.luke.spark.jss;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlOperatorExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Sql Operator Examples")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("JavaSparkSql/src/main/resources/employees.json");
        actionOperator(spark, df);
        tranformOperator(spark, df);
        spark.stop();
    }
    public static void actionOperator(SparkSession spark, Dataset<Row> df) {
        /*
        show : 展示数据
        参数：
            NumRows 要展示出的行数，默认为20行
            Truncate 取值为 true / false  ，表示一个字段是否最多显示20个字符， 默认为true
         */
        // 只显示20行记录，并且每个字段最多显示20个字符
        df.show();
        // 显示30行记录，并且每个字段最多显示20个字符
        df.show(30);
        // 显示10行记录，可以显示超过20个字符的产字符串格式的字段
        df.show(10, false);

        /*
        collect 获取所有数据到数组
         */
        df.collect();

        /*
        collectAsList 获取所有数据到List
         */
        df.collectAsList();

        /*
        describe 获取指定字段的统计信息
            count 记录条数
            Mean  平均值
            Stddev 样本标准差
            Min 最小值
            Max 最大值
         */
        df.describe("salary").show();

        /*
        first 获取第一行记录
         */
        df.first();

        /*
        head 获取第一行记录
        head （n） 获取前n行记录
         */
        df.head();
        df.head(10);

        /*
        take 获取前n行记录
         */
        df.take(20);

        /*
        takeAsList 获取前n行记录，并以List的形式展现
         */
        df.takeAsList(20);

    }
    public static void tranformOperator(SparkSession spark, Dataset<Row> df) {

    }
}
