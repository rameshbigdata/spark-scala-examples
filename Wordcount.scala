package com.ramesh.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args:Array[String]):Unit = {
    var conf=new SparkConf().setAppName("Word Count Program").setMaster("local[*]")
    var sc=new SparkContext(conf)
    var rdd=sc.textFile("/home/hadoop/work/spark_inputs/textinput").flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_).sortByKey(true, 1)
    rdd.saveAsTextFile("/home/hadoop/work/spark_output/wc_result")
    
  }
}