package com.aliyun.odps.spark.examples

import org.apache.spark.sql.SparkSession

object MAU {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DAU")
      .config("spark.master", "local[4]")// 需设置spark.master为local[N]才能直接运行，N为并发数
      .config("spark.hadoop.odps.project.name", "graviti_bi")
      .config("spark.hadoop.odps.access.id", "LTAI5tE3bzCSuJ2QY5dfGNa8")
      .config("spark.hadoop.odps.access.key", "QCqKValsq6IzU2G7UvO8Cxr1vPoXsY")
      .config("spark.hadoop.odps.end.point", "http://service.cn.maxcompute.aliyun.com/api")
      .config("spark.sql.catalogImplementation", "odps")
      .getOrCreate()

    try{
      //通过SparkSql查询表
      val data = spark.sql("select year(insert_time), month(insert_time), count(distinct id) as mau " +
        "from etl_user")
      //展示查询数据
      data.show()
    }finally {
      spark.stop()
    }

  }
}
