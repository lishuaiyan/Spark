package net.duxingzhe.luke.spark.jss;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author luke yan
 * @Description Java Spark Sql Dataset Example
 * @CreateDate 2020/09/09
 * @UpdateDate 2020/09/09
 */
public class SparkSqlDatasetExample {
    public static class Person implements Serializable {
        private String name;
        private Long age;

        public Person(String name, Long age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getAge() {
            return age;
        }

        public void setAge(Long age) {
            this.age = age;
        }
    }
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Sql Dataset Example")
                .master("local[*]")
                .getOrCreate();
        // 创建Dataset<Person>
        List<Person> personList = new ArrayList<Person>();
        personList.add(new Person("Andy", 32L));
        personList.add(new Person("Amy", 23L));
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDS = spark.createDataset(
                personList, personEncoder
        );
        personDS.show();

        // 创建Dataset<Integer>
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect();
        // 可以通过制定类名的方式将DataFrame对象转化为对应类的Dataset对象
        String path = "JavaSparkSql/src/main/resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();

        // Dataset 版 WordCount
        // 读取数据源，转化生成 Dataset<String>
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> data = spark.read().text("JavaSparkSql/src/main/resources/data.txt").as(stringEncoder);
        // 分割单词并且对单词进行分组
        Dataset<String> words = data.flatMap((FlatMapFunction<String,String>) value -> Arrays.asList(value.split("\\s+")).iterator(),
                stringEncoder);
        words.printSchema();

        //分组时，我们没有像RDD版WordCount创建出一个（key, value）键值对，因为Dataset是工作在行级别的抽象，每个元素将被看作是带有多列的行数据，而且都可以看作
        // 是groupByKey操作的key
        KeyValueGroupedDataset<String, String> groupedWords = words.groupByKey((MapFunction<String, String>) String::toLowerCase, stringEncoder);
        long counts = groupedWords.keys().count();
        System.out.println(counts);




    }
}
