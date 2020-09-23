package net.duxingzhe.luke.spark.jss;


import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;


import java.io.Serializable;

/**
 * @author luke yan
 * @Description 用户自定义强类型聚合函数
 */
public class JavaUserDefinedTypedAggregation {
    /**
     * 定义Employee 类规范聚合函数输入数据的数据类型
     */
    public static class Employee implements Serializable {
        private String name;
        private long salary;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }
    }

    /**
     * 定义Average类规范buffer聚合缓冲器的数据类型
     */
    public static class Average implements Serializable {
        private long sum;
        private long count;

        public Average() {}
        public Average(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    /**
     * 用户定义的强类型聚合函数必须继承Aggregator抽象类，注意需传入聚合函数输入数据、buffer缓冲器及返回结果的泛型数据
     */
    public static class MyAverage extends Aggregator<Employee, Average, Double> {

        /**
         * 定义聚合的零值，应满足任何 b + zero = b
         * @return
         */
        @Override
        public Average zero() {
            return new Average(0L, 0L);
        }

        /**
         * 1、定义作为Average对象的buffer聚合缓冲器如何处理每一条输入数据（Employee对象）的聚合逻辑。
         * 2、与求取平均值的无类型聚合函数的update方法一样，每一次调用reduce都会更新buffer聚合缓冲器的值，并将更新后的buffer作为返回值
         * @param buffer
         * @param employee
         * @return
         */
        @Override
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }

        /**
         * 与求取平均值的无类型聚合函数的merge的方法实现的逻辑相同
         * @param b1
         * @param b2
         * @return
         */
        @Override
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }

        /**
         * 定义输出结果的逻辑
         * reduction 表示 buffer聚合缓冲器经过多次reduce、merge之后的最终的最终聚合结果
         * 仍为Average对象记录着所有数据的累加和、累加次数
         * @param reduction
         * @return
         */
        @Override
        public Double finish(Average reduction) {
            return ((double) reduction.getSum())  / reduction.getCount();
        }

        /**
         * 指定中间值的编码器类型
         * @return
         */
        @Override
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        /**
         * 指定最终输出值的编码器类型
         * @return
         */
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL user-defined Datasets aggregation example")
                .master("local[*]")
                .getOrCreate();
        Encoder<Employee>  employeeEncoder = Encoders.bean(Employee.class);

        String path = "JavaSparkSql/src/main/resources/employees.json";

        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();
        MyAverage myAverage = new MyAverage();
        // 将函数转换为"TypedColumn",并给它一个名称
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();

        spark.stop();

    }
}
