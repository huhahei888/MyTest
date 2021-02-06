package com.bj58.dsap.job

import org.apache.spark.{SparkConf, SparkContext}

object CommonWeChatFriendsSparkContext {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[1]").setAppName("test_data_sdf")
    val context = new SparkContext(conf)
    val inPath = args(0)
    val outPath = args(1)
    val partitionNUm = Integer.valueOf(args(2))
    val rawData = context.textFile(inPath, partitionNUm)
    val cacheRdd = rawData.cache()
    val resultData = cacheRdd.flatMap(line => {
      line.split("\t")
    }).map(id => (id, 1)).reduceByKey((x, y) => x + y)
    resultData.map(x => {
      x._1 + "\t" + x._2
    }).saveAsTextFile(outPath)

  }
}