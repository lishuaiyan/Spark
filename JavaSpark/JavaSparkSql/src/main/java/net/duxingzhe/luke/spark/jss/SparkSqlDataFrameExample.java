package net.duxingzhe.luke.spark.jss;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.*;

import static org.apache.arrow.flatbuf.Type.Map;

/**
 * @author luke yan
 * @Description Spark Sql DataFrame Example
 * @CreateDate 2020/09/10
 * @UpdateDate 2020/09/10
 */
public class SparkSqlDataFrameExample {
    public static class Person implements Serializable {
        private String name;
        private Integer age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Sql DataFrame Example")
                .master("local[*]")
                .getOrCreate();


        // 调用 javaRDD 将Dataset<String> 转为 JavaRDD<String>
        JavaRDD<String> javaRDD = spark.read().textFile("JavaSparkSql/src/main/resources/person.txt")
                .javaRDD();
        /*
        使用反射机制推理schema 将javaRDD 转化为 DataFrame
         */
        JavaRDD<Person> personJavaRDD = javaRDD
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });
        // 将 JavaRDD<Person> 转为 DataFrame
        Dataset<Row> personDF = spark.createDataFrame(personJavaRDD, Person.class);
        // 将 personDF 注册为 临时表
        personDF.createOrReplaceTempView("people");
        // 调用 SparkSQL SQL 接口
        Dataset<Row> teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 10 AND 30");

        // Row 对象 支持通过字段索引进行访问
        teenagersDF.map((MapFunction<Row, String>) teenager -> "Name: " + teenager.getString(0), Encoders.STRING()).show();
        // 也支持直接通过列名进行访问
        teenagersDF.map((MapFunction<Row, String>) teenager -> "Name: " + teenager.<String>getAs("name"), Encoders.STRING()).show();

        /*
        开发者指定Schema 将 javaRDD 转化为 DataFrame
         */
        // 创建所需要的 schema
        String schemaString = "name age";
        // 将schemaString 按空格分隔返回字符串数组，对字符串数组进行遍历，并对数组里的每一个字符串进一步封装成StructField对象，进而构成List<StructField>
        List<StructField> fields = new ArrayList<>(2);
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        // 将fields 转换为 StructType对象，形成可用于构建DataFrame对象的schema
        StructType schema = DataTypes.createStructType(fields);
        // 将javaRDD<String> 转化为 javaRDD<Row>
        JavaRDD<Row> rowJavaRDD = javaRDD.map(line -> {
            String[] parts = line.split(",");
            return RowFactory.create(parts[0].trim(), parts[1].trim());
        });
        // 将schema应用到JavaRDD<Row>上，完成DataFrame的转换
        Dataset<Row> peopleDF = spark.createDataFrame(rowJavaRDD, schema);
        // 将peopleDF 注册为临时表
        peopleDF.createOrReplaceTempView("people2");
        // 调用 sql 接口 进行 SQL 查询
        Dataset<Row> results = spark.sql("SELECT name FROM people2");

        results.map((MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"), Encoders.STRING());



        spark.stop();
    }
}
