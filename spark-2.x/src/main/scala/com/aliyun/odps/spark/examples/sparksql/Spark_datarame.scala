package com.aliyun.odps.spark.examples.sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{datediff, substring, to_date}

object Spark_datarame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark2OSS")
      .config("spark.master", "local[4]")// 需设置spark.master为local[N]才能直接运行，N为并发数
      .config("spark.hadoop.odps.project.name", "graviti_bi")
      .config("spark.hadoop.odps.access.id", "LTAI5tE3bzCSuJ2QY5dfGNa8")
      .config("spark.hadoop.odps.access.key", "QCqKValsq6IzU2G7UvO8Cxr1vPoXsY")
      .config("spark.hadoop.odps.end.point", "http://service.cn.maxcompute.aliyun.com/api")
      .config("spark.sql.catalogImplementation", "odps")
      .getOrCreate()

    try{
      //通过SparkSql查询表
      val data = spark.read.format("csv").option("header","true")
        .option("inferSchema","true").load("etl_user.csv")
      //展示查询数据
      data.select("insert_time","id")
        .groupBy("id")
    }finally {
      spark.stop()
    }

  }
}



