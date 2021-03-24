package com.training.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkJoins {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Spark Assignment").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    /**
     * Intro
     */
    /*val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val rddcollect = rdd1.collect()

    println("num of partitions = " + rdd1.getNumPartitions)

    rddcollect.foreach(println)
    val rdd2 = sc.makeRDD(List(1, 2, 3, 4, 5)).toDF()
    rdd2.show()*/

    /**
     * Join
     */
    /*val deptdf = Seq(
      (1, "HR", "A"),
      (2, "Escalation", "B"),
      (3, "Engineering", "C")
    ).toDF("deptid", "deptname", "deptmgr")

    val empdf = Seq(
      (1, "A", 25000, 1),
      (2, "B", 30000, 2),
      (3, "C", 100000, 3),
      (4, "D", 50000, 3),
      (5, "E", 40000, 3)
    ).toDF("empid", "empname", "empsalary", "deptid")

    val joineddf = empdf.join(deptdf, Seq("deptid"))

    val joinedstatsdf = joineddf.groupBy("deptid").agg(
      max("empsalary").as("maxsalary"),
      min("empsalary").as("minsalary"),
      avg("empsalary").as("avgsalary")
    )

    deptdf.show()
    empdf.show()
    joineddf.show()
    joinedstatsdf.show()

    deptdf.explain(true)
    empdf.explain(true)
    joineddf.explain(true)
    joinedstatsdf.explain(true)*/

    /**
     * Map example 1
     */
    /*val empdf = Seq(
      (1, "A", 25000, 1),
      (2, "B", 30000, 2),
      (3, "C", 100000, 3),
      (4, "D", 50000, 3),
      (5, "E", 40000, 3)
    ).toDF("empid", "empname", "empsalary", "deptid")

    val emprdd = empdf.rdd

    val salaryhikeemprdd = emprdd.map(row => row.getInt(3) match {
      case 3 => (row.getInt(0), row.getString(1), row.getInt(2) + 5000, row.getInt(3))
      case _ => (row.getInt(0), row.getString(1), row.getInt(2) + 1000, row.getInt(3))
    })

    salaryhikeemprdd.collect().foreach(println)
    salaryhikeemprdd.toDF().explain(true)*/

    /**
     * Map example 2
     */
    /*val testdf = Seq(1, 2, 3, 1, 1, 5, 2, 3, 3, 4).toDF("key")
    val mappedtestdf = testdf.map(x => (x.getInt(0), 1)).toDF("key", "val")
    mappedtestdf.show()
    mappedtestdf.explain(true)*/

    /**
     * groupBy & groupByKey
     */
    /*val testdf = Seq(1, 2, 3, 1, 1, 5, 2, 3, 3, 4).toDF("key")
    val groupeddf1 = testdf.groupBy("key").count()
    val groupeddf2 = testdf.groupByKey(x => x.getInt(0)).count().toDF("key", "count")
    groupeddf1.show()
    groupeddf1.explain(true)
    groupeddf2.show()
    groupeddf2.explain(true)*/

    /**
     * flatMap
     */
    /*val testdf = Seq(
      ("Google", List("Chrome", "Gmail", "Drive", "Android", "YouTube")),
      ("Apple", List("iPhone", "iPad", "Mac", "Apple Music", "AirPods")),
      ("Samsung", List("Galaxy", "TV"))
    ).toDF("Company", "Products")
    val flatmappeddf = testdf.flatMap(x => x.getSeq[String](1).map((x.getString(0), _))).toDF("Company", "Product")
    flatmappeddf.show()
    flatmappeddf.explain(true)*/

    /**
     * countByKey
     */
    /*val testrdd = sc.makeRDD(Seq(1, 2, 3, 1, 1, 5, 2, 3, 3, 4))
    val rdd1 = testrdd.map(x => (x, 1))
    val rdd2 = rdd1.countByKey()
    println(rdd1.toDebugString)
    rdd2.foreach(println)*/

    /**
     * reduceByKey
     */
    /*val testrdd = sc.makeRDD(Seq(1, 2, 3, 1, 1, 5, 2, 3, 3, 4))
    val mappedrdd = testrdd.map(x => (x, 1))
    val reducedrdd = mappedrdd.reduceByKey(_ + _)
    println(reducedrdd.toDebugString)
    reducedrdd.collect.foreach(println)*/

    /**
     * aggregateByKey
     */
    /*val testrdd = sc.makeRDD(Seq(1, 2, 3, 1, 1, 5, 2, 3, 3, 4))
    val mappedrdd = testrdd.map(x => (x, 1))
    val aggregatedrdd = mappedrdd.aggregateByKey(0)(_ + _, _ + _)
    println(aggregatedrdd.toDebugString)
    aggregatedrdd.collect.foreach(println)*/

    /**
     * combineByKey - Dont completely understand it
     */
    /*val rdd1 = sc.makeRDD(List("dog", "cat", "salmon", "python", "wolf", "bear", "turtle", "bee"))
    val rdd2 = sc.makeRDD(List(1, 1, 2, 3, 1, 1, 2, 4))
    val rdd3 = rdd2.zip(rdd1)
    val combinedrdd = rdd3.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
    println(combinedrdd.toDebugString)
    combinedrdd.collect.foreach(println)*/

    /**
     * Union
     */
    /*val deptdf = Seq(
      (1, "HR", "A"),
      (2, "Escalation", "B"),
      (3, "Engineering", "C")
    ).toDF("deptid", "deptname", "deptmgr")

    val empdf = Seq(
      (1, "A", 25000),
      (2, "B", 30000),
      (3, "C", 100000),
      (4, "D", 50000),
      (5, "E", 40000)
    ).toDF("empid", "empname", "empsalary")

    val unioneddf = empdf.union(deptdf)
    unioneddf.show()
    unioneddf.explain(true)*/

    Thread.sleep(300000)
    sc.stop()
    spark.stop()
  }

  /*
""
   */
}
