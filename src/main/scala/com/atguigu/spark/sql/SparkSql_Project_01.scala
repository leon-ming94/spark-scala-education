package com.atguigu.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @author shkstart
  */
object SparkSql_Project_01 {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
                .builder()
                .appName("SparkSql_Project_01")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()
        import spark.implicits._

        val acUDAF: AreaClickUDAF = new AreaClickUDAF
        spark.udf.register("city_remark",acUDAF)

        spark.sql(
            """
              |SELECT
              |     area a,
              |	    pName p,
              |     areaProductClick r,
              |     note nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn
              |FROM (
              |	SELECT
              |		area,
              |	    pName,
              |     areaProductClick,
              |     note,
              |		RANK() OVER(PARTITION BY area,pName ORDER BY areaProductClick) rk
              |	from (
              |		SELECT
              |			area,
              |			pName,
              |			count(*) areaProductClick,
              |         city_remark(cName) note
              |		FROM (
              |			SELECT
              |             uva.*,
              |				ci.area area,
              |             pi.product_name pName,
              |				ci.city_name cName
              |			FROM
              |				sparkpractice.user_visit_action uva
              |			JOIN sparkpractice.product_info pi
              |			ON uva.click_category_id = pi.product_id
              |			JOIN sparkpractice.city_info ci
              |			ON uva.city_id = ci.city_id
              |			WHERE uva.click_category_id != -1
              |		)t1
              |		GROUP BY area,pName
              |	)t2
              |)t3
              |WHERE rk < 3
            """.stripMargin).show()

    }
}
