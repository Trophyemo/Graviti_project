package com.aliyun.odps.spark.examples.sparksql

import org.apache.spark.sql.SparkSession


object DAU_try {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DAU")
      .config("spark.sql.broadcastTimeout", 20 * 60)
      .config("spark.sql.crossJoin.enabled", value = true)
      .config("odps.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.catalogImplementation", "odps")
      .getOrCreate()

    val sc = spark.sparkContext

    try{
      //通过SparkSql查询表
      val data = spark.sql("select date(a.insert_time) as date, count(distinct a.id) as DAU, count(distinct b.id)/count(distinct a.id) as retention from etl_user a left join etl_user b on a.id=b.id and datediff(date(b.insert_time), date(a.insert_time)) = 1 group by date(a.insert_time) order by date(a.insert_time)")
      //展示查询数据
      data.show()
    }finally {
      spark.stop()
    }

  }
}
