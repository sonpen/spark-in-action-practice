package com.sonpen.chapter03

import org.apache.spark.sql.SparkSession

/**
  * Created by 1109806 on 2020/12/01.
  */
object App {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("Chapter03").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val homeDir = System.getenv("HOME")
    val path = homeDir + "/IdeaProjects/spark-in-action-practice/src/main/resources/2015-03-01-1.json"

    val ghLog = spark.read.json(path)

    val pushes = ghLog.filter("type = 'PushEvent'")

    pushes.printSchema()
    println("all events: " + ghLog.count())
    println("only pushes: " + pushes.count())
    pushes.show(5)

  }

}
