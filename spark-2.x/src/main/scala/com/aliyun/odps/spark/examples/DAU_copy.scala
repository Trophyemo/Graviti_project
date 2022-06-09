package com.aliyun.odps.spark.examples

import org.apache.spark.sql.SparkSession

object DAU_copy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DAU_copy")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("spark.master", "local[4]")// 需设置spark.master为local[N]才能直接运行，N为并发数
      .config("spark.hadoop.odps.project.name", "graviti_bi")
      .config("spark.hadoop.odps.access.id", "LTAI5tE3bzCSuJ2QY5dfGNa8")
      .config("spark.hadoop.odps.access.key", "QCqKValsq6IzU2G7UvO8Cxr1vPoXsY")
      .config("spark.hadoop.odps.end.point", "http://service.cn.maxcompute.aliyun.com/api")
      .config("spark.sql.catalogImplementation", "odps")
      .getOrCreate()

    try{
      import spark.implicits._
      //导入函数
      import org.apache.spark.sql.functions._
      val user_rdd = spark.sql("select * from etl_user")
      val data_date = user_rdd.withColumn("date", to_date(col("insert_time")))
      val data_id = data_date.select(col("date"), col("id"))
      data_id.createTempView("data_id")
      val joined = spark.sql("select a.date, a.id as reg_id, b.id as sign_id from data_id a " +
        "left join data_id b on a.id=b.id and datediff(b.id, a.id)=1")

      val data_reg = joined.groupBy("date").agg(countDistinct("reg_id").as("reg_num"))
        .select("date", "reg_num")
      val data_sign = joined.groupBy("date").agg(countDistinct("sign_id")).as("sign_num")
        .select("date", "sign_num")
      //两个表相关联
      val rejoined = data_reg.join(data_sign, Seq("date"))
      data_id.createTempView("rejoined")
      val DAU = spark.sql("select date, sign_num/reg_num from rejoined order by date")


      DAU.show()
    }finally {
      spark.stop()
    }

  }
}