package net.duxingzhe.luke.spark.jss;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author luke yan
 * @Description Java Spark Sql UDF
 * @CreateDate 2020/09/10
 * @UpdateDate 2020/09/10
 */
public class JavaUserDefinedUntypedAggregation {
    /**
     * 用户自定义的无类型聚合函数必须继承UserDefinedAggregateFunction抽象类
     */
    public static class MyAverage extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }

        /**
         * 聚合函数输入参数的数据类型（其实是该函数所作用的DataFrame指定列的数据类型）
         * @return
         */
        @Override
        public StructType inputSchema() {
            return inputSchema;
        }

        /**
         * 聚合函数的缓冲器结构，定义了用于记录累加值和累加数的字段结构
         * @return
         */
        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        /**
         * 聚合函数返回值的数据类型
         * @return
         */
        @Override
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        /**
         * 此函数是否始终在相同输入上返回相同输出
         * @return
         */
        @Override
        public boolean deterministic() {
            return true;
        }

        /**
         * 1、初始化给定的buffer聚合缓冲器
         * 2、buffer聚合缓冲器其本身是一个 Row 对象，因此可以调用标准方法访问buffer内的元素，例如在索引处检索一个值，也可以根据索引更新其值
         * 3、需要注意的是buffer内的 Array、Map对象仍是不可变的
         * @param buffer
         */
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L);
            buffer.update(1, 0L);

        }

        /**
         * update函数负责将input代表的输入数据更新到buffer聚合缓冲器中，buffer缓冲器记录着累加和buffer(0) 与 累加数 buffer(1)
         * @param buffer
         * @param input
         */
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long updatedSum = buffer.getLong(0) + input.getLong(0);
                long updatedCount = buffer.getLong(1) + 1;
                buffer.update(0, updatedSum);
                buffer.update(1, updatedCount);
            }

        }

        /**
         * 1、合并两个buffer聚合缓冲器的部分累加和、累加次数，更新到buffer1主聚合缓冲器
         * 2、buffer1位主聚合缓冲器，其代表各个节点得到的部分结果经聚合后得到的最终结果
         * 3、buffer2代表着各个分布式任务执行节点的部分执行结果
         * 4、merge() 的重写实质上是实现buffer1与多个buffer2的合并逻辑
         * @param buffer1
         * @param buffer2
         */
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergeSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergeCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergeSum);
            buffer1.update(1, mergeCount);

        }

        /**
         * 计算最终结果
         * @param buffer
         * @return
         */
        @Override
        public Object evaluate(Row buffer) {
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }
    }
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL user-defined DataFrames aggregation example")
                .getOrCreate();

        // 注意：想要在sql语句中使用用户自定义函数，必须先将函数进行注册
        spark.udf().register("myAverage", new MyAverage());

        Dataset<Row> df = spark.read().json("");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
        spark.stop();
    }
}
