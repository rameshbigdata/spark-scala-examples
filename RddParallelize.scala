package com.ramesh.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object RddParallelize extends App {

  var sc=new SparkContext(new SparkConf().setMaster("local[1]").setAppName("RDD Parallelize"))
  var rdd:RDD[Int]=sc.parallelize(Array(1,2,3,4,5,6))
  var returnArray:Array[Int]=rdd.collect()
  println("Get number of partitions "+rdd.getNumPartitions)
  println("Get First Record "+rdd.first())
  println("Records are ***************->")
  rdd.collect().foreach(println)
  /* Create Empty RDD in Spark*/
  var emptyRdd=sc.emptyRDD[String]
  println("Check rdd is empty or not "+emptyRdd.isEmpty())
  var emptyRDD1=sc.parallelize(Seq.empty[String])
  println(emptyRDD1.isEmpty())
  sc.stop()
}