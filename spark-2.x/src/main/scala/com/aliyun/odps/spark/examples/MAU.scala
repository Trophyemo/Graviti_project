package com.aliyun.odps.spark.examples

import org.apache.spark.sql.SparkSession

object MAU {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DAU")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("spark.master", "local[4]")// 需设置spark.master为local[N]才能直接运行，N为并发数
      .getOrCreate()

    try{
      //通过SparkSql查询表
      val data = spark.sql("select  concat(year(a.insert_time),month(a.insert_time)), count(distinct a.id) as MAU, count(distinct b.id) as MAU_sec, count(distinct b.id)/count(distinct a.id) as Monthly_retention from etl_user a left join etl_user b on a.id=b.id and ((year(b.insert_time)-year(a.insert_time))*12 + month(b.insert_time)-month(a.insert_time)) = 1 group by concat(year(a.insert_time),month(a.insert_time)) order by concat(year(a.insert_time),month(a.insert_time))")
      //展示查询数据
      data.show()
    }finally {
      spark.stop()
    }

  }
}
