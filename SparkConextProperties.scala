package com.ramesh.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkConextProperties extends App {

  var conf=new SparkConf().setAppName("Spark ContextProperties").setMaster("local[*]")
 
  var sc=new SparkContext(conf)
  println("Spark User "+sc.sparkUser)
  println("Spark AppName "+sc.appName)
  println("Spark default parallelism "+sc.defaultParallelism)
  println("Spark Conf "+sc.getConf)
  println(sc.setLogLevel("ERROR"))
  println("Spark Version number "+sc.version)
  println("Spark Deploymode "+sc.deployMode)
  println("Spark Master "+sc.master)
  sc.stop()
  
  
}