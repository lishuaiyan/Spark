package net.duxingzhe.luke.spark.jss;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * @author luke yan
 * @Description Java Spark Sql Action and Transform Operator
 * @CreateDate 2020/09/11
 * @UpdateDate 2020/09/11
 */
public class SparkSqlOperatorExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Sql Operator Examples")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

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
        /*
        where SQL 语言中where关键字后的条件
         */
        df.where("name = 'Michael' or salary = 3500").show();
        df.where("name = 'Michael' and salary = 3000").show();

        /*
        filter 根据字段进行筛选
         */
        df.filter("name = 'Michael' or salary = 3500").show();
        df.filter(col("salary").gt(3000)).show();

        /*
        select 获取指定字段值
         */
        df.select("name", "salary").show(false);
        df.select(col("name"), col("salary").plus(1000)).show();
        df.select(expr("name"), expr("salary as `sal`"), expr("round(salary)")).show();
        /*
        selectExpr 可以对指定字段进行特殊处理
         */
        df.selectExpr("name", "salary as `sal`", "round(salary)" ).show();

        /*
        col 获取指定字段
         */
        df.col("name");

        /*
        apply 获取指定列
         */
        df.apply("name");

        /*
        drop 去除指定字段， 保留其他字段
         */

        df.drop("salary").show();

        /*
        orderBy 和 sort 按指定字段排序，默认升序
         */
        df.orderBy(col("salary").asc()).show();
        df.orderBy(col("salary").desc()).show();
        df.orderBy(col("salary").asc(), col("name")).show();

        df.sort(col("salary").asc(), col("name")).show();

        /*
        sortWithPartitions 返回的是排好序的每一个Partition 的 DataFrame对象
         */

        /*
        groupBy 根据字段进行 group by 操作
         */
        df.groupBy("salary");
        df.groupBy(col("salary"));
        df.groupBy("salary").max().show();
        df.groupBy("salary").count().show();
        df.groupBy("salary").mean().show();
        df.groupBy("name").mean("salary").show();

        /*
        distinct 返回一个不包含重复记录的DataFrame
         */
        df.distinct().show();

        /*
        dropDuplicates 根据指定字段进行去重
         */
        df.dropDuplicates("name", "salary").show();

        /*
        agg 该方法输入的是对于聚合操作的表达。可同时对多个列进行聚合操作。一般与 groupBy 方法配合使用
         */
        df.agg(mean("salary")).show();
        df.groupBy("name").agg(mean("salary")).show();

        /*
        union 对两个字段一致的DataFrame进行组合，返回是组合生成的新DataFrame，类似于SQL中的 UNION操作
         */
        df.limit(5).union(df.limit(5));

        /*
        join 操作
         */
        // 单字段join
        df.join(df, "name").show(5);
        // 多字段join
        df.join(df,
                df.col("name").equalTo(df.col("name"))
                        .and(
                                df.col("salary").equalTo(df.col("salary"))
                        )
        ).show();

        // 指定join类型
        df.join(df, df.col("name").equalTo(df.col("name")), "inner_join").show(5);
        df.join(df,
                df.col("name").equalTo(df.col("name"))
                        .and(
                                df.col("salary").equalTo(df.col("salary"))
                        ),
                "left_outer"
        ).show();

        /*
        stat 用于计算 指定字段或指定字段之间的统计信息， 比如方差、协方差、某字段出现频繁的元素集合等，这个方法返回DataFrameStatFunctions 类型对象
         */
        // 根据age、height字段，统计该字段值出现频率在30%以上的内容
        df.stat().freqItems(new String[]{"age", "salary"}, 0.3).show();

        // corr 求两列相关性演示
        df.stat().corr("height", "weight");

        // cov 求两列协方差

        df.stat().cov("height", "weight");

        /*
        intersect 方法可以计算两个DataFrame中相同的记录，返回值也为 DataFrame
         */
        df.intersect(df.limit(1)).show();
        /*
        except 获取一个DataFrame 中有 另一个 DataFrame 中没有的记录
         */
        df.except(df.limit(2)).show();

        /*
        withColumnRenamed 重命名DataFrame 中的指定字段名
         */
        df.withColumnRenamed("name", "englishName");

        /*
        withColumn 往当前DataFrame中新增一列，该列可来源于本身的DataFrame对象，不可来自其他非己DataFrame对象
        withColumn(String colName, Column col) 方法根据指定colName往DataFrame中新增一列，如果colName已存在，则会覆盖当前列
         */
        df.withColumn("age*2", col("age").multiply(2)).show();

        /*
        na 该方法返回的是对应的DataFrameNaFunction对象，进而调用对应的drop、fill方法来处理指定列为空值的行
         */
        // 只要具有空值列的行数据就会被删除
        df.na().drop().show();
        df.na().drop(new String[]{"name"}).show();
        // fill 使用指定的值替换指定空值列的值
        Map<String, Object> map = new HashMap<>();
        map.put("name", "non");
        map.put("age", 0);
        df.na().fill(map).show();
    }
}
