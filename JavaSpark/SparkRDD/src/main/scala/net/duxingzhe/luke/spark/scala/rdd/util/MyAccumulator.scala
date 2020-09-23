package net.duxingzhe.luke.spark.scala.rdd.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

/**
 * 继承Accumulator2， 指定输入类型为String 输出为 ArrayBuffer[String]
 */
class MyAccumulator extends AccumulatorV2[String, ArrayBuffer[String]]{
  // 设置累加器的结果，类型为ArrayBuffer[String]
  private var result = ArrayBuffer[String]()

  override def isZero: Boolean = this.result.isEmpty

  /**
   * copy方法设置新的累加器，并把当前result赋给新的累加器
   * @return
   */
  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = {
    val newAccum = new MyAccumulator
    newAccum.result = this.result
    newAccum
  }

  /**
   * reset方法把当前result设置成新的ArrayBuffer
   */
  override def reset(): Unit = this.result == new ArrayBuffer[String]()

  /**
   * add方法把传进来的字符串加到result内
   * @param v
   */
  override def add(v: String): Unit = this.result += v

  /**
   * merge方法把两个累加器的result合并起来
   * @param other
   */
  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    result.++=:(other.value)
  }

  /**
   * value方法返回当前的result
   * @return
   */
  override def value: ArrayBuffer[String] = this.result
}
