package com.atguigu.spark.core

import java.util
import java.util.Collections

import org.apache.spark.util.AccumulatorV2

/**
  * @author shkstart
  */
object MyAccDemo {


}

class MyAcc extends AccumulatorV2[String, java.util.List[String]] {

    //TODO 创建一个线程安全的List集合
    //TODO 累加器在转换算子中有可能不止执行一遍
    private val _list: java.util.List[String] = Collections.synchronizedList(new util.ArrayList[String]())

    override def isZero: Boolean = _list.isEmpty

    override def copy(): AccumulatorV2[String, util.List[String]] = {
        val newAcc = new MyAcc
        _list.synchronized {
            newAcc._list.addAll(_list)
        }
        newAcc
    }

    override def reset(): Unit = _list.clear()

    override def add(v: String): Unit = _list.add(v)

    override def merge(other: AccumulatorV2[String, util.List[String]]): Unit = other match {
        case o: MyAcc => _list.addAll(o.value)
        case _ => throw new UnsupportedOperationException(
            s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

    override def value: util.List[String] = java.util.Collections.unmodifiableList(new util.ArrayList[String](_list))
}
