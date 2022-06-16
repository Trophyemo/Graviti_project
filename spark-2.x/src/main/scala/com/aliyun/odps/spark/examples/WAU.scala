package com.aliyun.odps.spark.examples

import org.apache.spark.sql.SparkSession

object WAU {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WAU")
      .config("spark.master", "local[4]") // 需设置spark.master为local[N]才能直接运行，N为并发数
      .getOrCreate()

    try {
      //通过SparkSql查询表
      val data = spark.sql("select date(insert_time) as date, id from etl_user group by date(insert_time) order by date(insert_time)")
      data.show()
    } finally {
      spark.stop()
    }
  }
}
