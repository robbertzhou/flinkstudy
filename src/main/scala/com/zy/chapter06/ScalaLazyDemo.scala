package com.zy.chapter06

/**
  * @create 2020-01-27
  * @author zhouyu
  * @desc scala懒加载关键字练习
  */
object ScalaLazyDemo {
  def init():String={
    println("zhouyu 666")
    "zhouyu"
  }
  def main(args: Array[String]): Unit = {
    lazy val name = init()
    println("666")
    println(name)
  }
}
